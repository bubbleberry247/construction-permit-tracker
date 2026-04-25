/**
 * MlitRateLimit.gs — MLIT (etsuran2) への全アクセスを 3 秒以上の間隔で機能横断にシリアライズする。
 *
 * GPT-5.4 レビュー指摘 #3: rolling バッチ内のローカル sleep だけでは不足で、
 *   - 単発検索 (logic.gs:searchMlit_)
 *   - rolling 再確認 (MlitRolling.gs:refreshOneMlitPermit_)
 *   - その他の経路
 * これら全てが共有 IP からの呼び出しなので、機能横断のレート制御が必須。
 *
 * 実装方針:
 *   1. ScriptLock を短時間だけ取って ScriptProperties の最終呼出時刻を atomic に更新
 *   2. 必要な sleep 時間を算出してロック解放後に sleep（ロックは長時間握らない）
 *   3. 実 fn() はロックなしで呼ぶ（30秒等の長時間処理が他機能をブロックしない）
 *
 * 使い方:
 *   var html = withMlitRateLimit_(function() {
 *     return UrlFetchApp.fetch(MLIT_SEARCH_URL_, options);
 *   });
 */

// ---------------------------------------------------------------------------
// 定数
// ---------------------------------------------------------------------------

var MLIT_RATE_LIMIT_KEY_ = 'MLIT_LAST_CALL_RESERVED_MS';
var MLIT_RATE_LIMIT_INTERVAL_MS_ = 3000; // 規約: 1件 3秒間隔

// ---------------------------------------------------------------------------
// 共通ラッパー
// ---------------------------------------------------------------------------

/**
 * MLIT 呼び出しを機能横断で 3 秒間隔にシリアライズして実行する。
 *
 * 動作:
 *   - 内部で「次に呼んでよい時刻」を ScriptProperties に予約
 *   - 並行呼び出しがあっても予約時刻が後ろにずれる（FIFO）
 *   - ロックは予約計算の数百 ms だけ。fn() 実行中は他の予約も進められる
 *
 * @param {Function} fn  実 MLIT 呼び出し（UrlFetchApp.fetch 等）。戻り値はそのまま返す
 * @return {*}
 * @throws {Error}  ロック取得失敗時
 */
function withMlitRateLimit_(fn) {
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
