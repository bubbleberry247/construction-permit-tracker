/**
 * MlitRateLimit.gs — MLIT (etsuran2) への全アクセスを 3 秒以上の間隔で機能横断にシリアライズする。
 *
 * GPT-5.4 レビュー反映点:
 *   - 機能横断シリアライズ: 単発検索/rolling 等を共通の予約スロットで直列化
 *   - **バックプレッシャー** (再レビュー指摘): 待機が一定を超える場合は予約せず即 throw。
 *     UI 連打や悪用で実行が無制限に滞留して 6分上限を消費するリスクを防ぐ。
 *
 * 実装方針:
 *   1. ScriptLock を短時間だけ取って ScriptProperties の予約スロット（次に呼んでよい時刻）を更新
 *   2. 算出した待機が opts.maxWaitMs を超える場合は予約せず即 throw（バックプレッシャー）
 *   3. 必要な sleep 時間を算出してロック解放後に sleep（ロックは長時間握らない）
 *   4. 実 fn() はロックなしで呼ぶ（30秒等の長時間処理が他機能をブロックしない）
 *
 * 使い方:
 *   // ユーザー対面（混雑時は即エラー）: 5秒以上待たされる場合は throw
 *   var html = withMlitRateLimit_(function() {
 *     return UrlFetchApp.fetch(MLIT_SEARCH_URL_, options);
 *   }, { maxWaitMs: 5000 });
 *
 *   // バックグラウンド（rolling など、混雑も許容）:
 *   var html = withMlitRateLimit_(function() { ... }, { maxWaitMs: 60000 });
 */

// ---------------------------------------------------------------------------
// 定数
// ---------------------------------------------------------------------------

var MLIT_RATE_LIMIT_KEY_ = 'MLIT_LAST_CALL_RESERVED_MS';
var MLIT_RATE_LIMIT_INTERVAL_MS_ = 3000; // 規約: 1件 3秒間隔

// 既定の最大待機。これを超える場合は予約せず即 throw する。
//   ユーザー対面: 5秒（5社が同時連打したら 6 番目以降は MLIT_QUEUE_FULL）
//   rolling 内部: 60秒（5件/日 × 3秒なら待機は最悪 12 秒程度なので余裕）
var MLIT_RATE_LIMIT_DEFAULT_MAX_WAIT_MS_ = 60000;

// バックプレッシャー throw のエラーコード
var MLIT_QUEUE_FULL_ERROR_ = 'MLIT_QUEUE_FULL';

// ---------------------------------------------------------------------------
// 共通ラッパー
// ---------------------------------------------------------------------------

/**
 * MLIT 呼び出しを機能横断で 3 秒間隔にシリアライズして実行する。
 *
 * 動作:
 *   - 内部で「次に呼んでよい時刻」を ScriptProperties に予約
 *   - 並行呼び出しがあっても予約時刻が後ろにずれる（FIFO）
 *   - 算出した待機が opts.maxWaitMs を超えたら予約せず throw（バックプレッシャー）
 *   - ロックは予約計算の数百 ms だけ。fn() 実行中は他の予約も進められる
 *
 * @param {Function} fn  実 MLIT 呼び出し（UrlFetchApp.fetch 等）。戻り値はそのまま返す
 * @param {{maxWaitMs?: number}} [opts]  オプション
 *   - maxWaitMs: 待機予約がこれを超えたら即 throw（既定 60000ms）
 * @return {*}
 * @throws {Error}  ロック取得失敗時、または MLIT_QUEUE_FULL（バックプレッシャー）
 */
function withMlitRateLimit_(fn, opts) {
  opts = opts || {};
  var maxWaitMs = (typeof opts.maxWaitMs === 'number')
    ? opts.maxWaitMs
    : MLIT_RATE_LIMIT_DEFAULT_MAX_WAIT_MS_;

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(15000)) {
    throw new Error('withMlitRateLimit_: ScriptLock 取得失敗（他処理が長時間ロック中の可能性）');
  }

  var sleepNeededMs = 0;
  try {
    var props = PropertiesService.getScriptProperties();
    var reservedStr = props.getProperty(MLIT_RATE_LIMIT_KEY_);
    var reserved = reservedStr ? parseInt(reservedStr, 10) : 0;
    if (isNaN(reserved)) reserved = 0;
    var now = Date.now();
    // 「次に呼んでよい時刻」を Math.max(now, 直前予約時刻 + 3000) で計算
    var nextSlotMs = Math.max(now, reserved + MLIT_RATE_LIMIT_INTERVAL_MS_);
    sleepNeededMs = nextSlotMs - now;

    // バックプレッシャー: 待機が上限を超える場合は予約せず即 throw
    // setProperty より前で throw するので「幽霊予約」は残らない
    if (sleepNeededMs > maxWaitMs) {
      throw new Error(
        MLIT_QUEUE_FULL_ERROR_ +
        ': MLIT への問い合わせが混雑しています（待機予約 ' + sleepNeededMs + 'ms > 上限 ' + maxWaitMs + 'ms）。' +
        '少し時間をおいてから再度お試しください。'
      );
    }

    // 予約をすぐ書く（他の呼び出しはこれを基準に更にずれる）
    props.setProperty(MLIT_RATE_LIMIT_KEY_, String(nextSlotMs));
  } finally {
    lock.releaseLock();
  }

  // ロック解放後に必要な sleep（他処理をブロックしない）
  if (sleepNeededMs > 0) {
    Utilities.sleep(sleepNeededMs);
  }

  // 実呼び出し（ロックなし）
  return fn();
}

/**
 * 緊急用: rate limit 予約をリセット（ScriptProperties から削除）
 * 起動直後など「予約が古すぎる」場合の救済
 */
function resetMlitRateLimit() {
  PropertiesService.getScriptProperties().deleteProperty(MLIT_RATE_LIMIT_KEY_);
  Logger.log('MLIT rate limit 予約をリセットしました');
}
