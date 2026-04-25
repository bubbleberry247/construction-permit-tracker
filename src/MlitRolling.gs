/**
 * MlitRolling.gs — MLITPermits の許可情報を毎日少量ずつローリング再確認する。
 *
 * 設計（GPT-5.4 レビュー2回反映済み）:
 *   - データソース: MLITPermits シート（fetch_status / last_synced 列あり）
 *   - 再確認対象: MANUAL_FETCH_STATUSES_ 以外の行で last_synced 古いもの優先
 *   - 巡回粒度: 1日 DAILY_LIMIT 件（既定 5）。145社÷5=29日で1巡
 *   - 起点: time-driven trigger（毎日 02:00 等）
 *   - 結果書き戻し: 更新前に authority + permit_number で行を **再解決**してから updateRecord_
 *   - kill switch: ScriptProperties の MLIT_ROLLING_PAUSE='true' で即停止
 *   - レート制限: MlitRateLimit.gs:withMlitRateLimit_ で機能横断シリアライズ（3秒間隔）
 *
 * GPT-5.4 レビュー反映点:
 *   #1 失敗時の終端化バグ防止: 一時失敗（network/HTML差分）は fetch_status を変えず last_synced のみ更新
 *      → 次回 stale 判定で再対象になり自然リトライ
 *   #2 _row 誤行更新防止: refreshOneMlitPermit_ 内の updateRecord_ 直前に
 *      authority + permit_number で行を再解決
 *   #3 機能横断レート制御: withMlitRateLimit_ 経由で MLIT 呼び出し
 *
 * 関連ファイル:
 *   - MlitSearch.gs (searchMlitPermit_/fetchMlitDetail_)
 *   - MlitRateLimit.gs (withMlitRateLimit_)
 *   - Scheduler.gs (既存日次バッチ、別 trigger)
 */

// ---------------------------------------------------------------------------
// 定数
// ---------------------------------------------------------------------------

var MLIT_ROLLING_DEFAULTS_ = {
  DAILY_LIMIT: 5,        // 1回の実行で再確認する最大件数
  MAX_STALE_DAYS: 30     // 最終同期からこの日数以上経過したものを対象
};

var MLIT_ROLLING_PAUSE_KEY_ = 'MLIT_ROLLING_PAUSE';

/**
 * 候補から除外する fetch_status（人手介入で確定したもの）
 *
 * MANUAL_ENTRY: 手入力で確定した値（MLIT 検索失敗時の代替）
 * DUPLICATE_DELETE: 重複として削除マーク
 * PERSON_NAME_DELETE: 個人事業主削除マーク
 * （PERMIT_CORRECTED_SEE_ROW* は prefix チェックで別扱い）
 *
 * NOT_FOUND_AT_REFRESH / TRANSIENT_ERROR は **含めない**（再試行対象）
 */
var MANUAL_FETCH_STATUSES_ = ['MANUAL_ENTRY', 'DUPLICATE_DELETE', 'PERSON_NAME_DELETE'];

// ---------------------------------------------------------------------------
// kill switch
// ---------------------------------------------------------------------------

function isMlitRollingPaused_() {
  var v = PropertiesService.getScriptProperties().getProperty(MLIT_ROLLING_PAUSE_KEY_);
  return String(v || '').toLowerCase() === 'true';
}

function pauseMlitRolling() {
  PropertiesService.getScriptProperties().setProperty(MLIT_ROLLING_PAUSE_KEY_, 'true');
  Logger.log('MLIT rolling は一時停止されました');
}

function resumeMlitRolling() {
  PropertiesService.getScriptProperties().deleteProperty(MLIT_ROLLING_PAUSE_KEY_);
  Logger.log('MLIT rolling は再開されました');
}

// ---------------------------------------------------------------------------
// 候補選択
// ---------------------------------------------------------------------------

/**
 * 再確認対象の MLITPermits 行を優先度順で返す。
 *
 * 候補:
 *   - fetch_status が MANUAL_FETCH_STATUSES_ または PERMIT_CORRECTED_* で **ない**
 *     （= OK / NOT_FOUND_AT_REFRESH / TRANSIENT_ERROR / その他は再試行対象）
 *   - AND last_synced が空 or maxStaleDays 以上経過
 *
 * 並び順: last_synced 昇順（古いものから優先）
 *
 * @param {number} limit
 * @param {number} maxStaleDays
 * @return {Object[]} MLITPermits 行オブジェクト（_row 含む）
 */
function pickMlitRollingCandidates_(limit, maxStaleDays) {
  var permits = readRecords_(SHEETS.MLITPermits);
  var staleCutoffMs = Date.now() - maxStaleDays * 86400000;

  var candidates = permits.filter(function(p) {
    var st = String(p.fetch_status || '').trim();
    if (MANUAL_FETCH_STATUSES_.indexOf(st) >= 0) return false;
    if (st.indexOf('PERMIT_CORRECTED_') === 0) return false;

    var ls = String(p.last_synced || '').trim();
    if (!ls) return true; // 同期日不明は最優先で再確認
    var d = new Date(ls);
    if (isNaN(d.getTime())) return true;
    return d.getTime() < staleCutoffMs;
  });

  candidates.sort(function(a, b) {
    var av = String(a.last_synced || '').trim();
    var bv = String(b.last_synced || '').trim();
    var ad = av ? new Date(av).getTime() : 0;
    var bd = bv ? new Date(bv).getTime() : 0;
    if (isNaN(ad)) ad = 0;
    if (isNaN(bd)) bd = 0;
    return ad - bd;
  });

  return candidates.slice(0, limit);
}

// ---------------------------------------------------------------------------
// 行再解決（_row 誤行更新防止）
// ---------------------------------------------------------------------------

/**
 * authority + permit_number で MLITPermits の行番号を再取得する。
 * 楽観ロックではなくシート手動編集対策（GPT-5.4 レビュー #2）。
 *
 * @param {string} authority
 * @param {string} permitNumber
 * @return {number}  1-indexed 行番号。見つからない場合は -1
 */
function resolveMlitPermitRow_(authority, permitNumber) {
  var auth = String(authority || '').trim();
  var pnum = String(permitNumber || '').trim();
  if (!auth || !pnum) return -1;

  var permits = readRecords_(SHEETS.MLITPermits);
  for (var i = 0; i < permits.length; i++) {
    if (String(permits[i].authority).trim() === auth &&
        String(permits[i].permit_number).trim() === pnum) {
      return permits[i]._row;
    }
  }
  return -1;
}

// ---------------------------------------------------------------------------
// 1件再確認
// ---------------------------------------------------------------------------

/**
 * 1件の MLITPermits 行を再確認して、結果を MLITPermits に書き戻す。
 *
 * 失敗パターンと fetch_status の扱い（GPT-5.4 レビュー #1 反映）:
 *   - 検索 throw: 一時障害。fetch_status は変えず last_synced 更新（次 stale で再試行）
 *   - 候補 0 件: 「以前 OK だったが今見つからない」。NOT_FOUND_AT_REFRESH に遷移するが
 *               候補抽出で除外されないので、stale で再試行されて復活も検出可能
 *   - 詳細 fetch throw: 一時障害。fetch_status 変えず last_synced 更新
 *   - 詳細 found=false: パース失敗。一時障害扱い、fetch_status 変えず last_synced 更新
 *   - 成功: fetch_status='OK' + 全フィールド更新
 *
 * @param {Object} permit  MLITPermits 行オブジェクト（候補抽出時のスナップショット）
 * @return {Object} {result: '一致'|'不一致'|'確認不可'|'スキップ', message: string}
 */
function refreshOneMlitPermit_(permit) {
  var authority = String(permit.authority || '').trim();
  var permitNumber = String(permit.permit_number || '').trim();
  var nowStr = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');

  if (!authority || !permitNumber) {
    return { result: '確認不可', message: 'authority/permit_number 欠損' };
  }

  // 更新時に使う行番号は **再解決** する（候補時点の _row はシート編集でずれている可能性）
  // ヘルパ: 失敗時の last_synced のみ更新
  var updateLastSyncedOnly = function() {
    var freshRow = resolveMlitPermitRow_(authority, permitNumber);
    if (freshRow < 0) return false;
    updateRecord_(SHEETS.MLITPermits, freshRow, { last_synced: nowStr });
    return true;
  };

  var licenseNoKbn = getLicenseNoKbn_(authority);
  var prefCode = getPrefCode_(authority);

  // バックグラウンド処理なのでユーザー対面より長めに待つが、無制限はダメ。
  // 30 秒超 = 既に 10 件以上が前にある = MLIT_QUEUE_FULL で諦め、次回 stale 再試行。
  var ROLLING_MAX_WAIT_MS = 30000;

  // 検索 API
  var candidates;
  try {
    candidates = withMlitRateLimit_(function() {
      return searchMlitPermit_(licenseNoKbn, permitNumber, prefCode);
    }, { maxWaitMs: ROLLING_MAX_WAIT_MS });
  } catch (e) {
    // 一時障害扱い（QUEUE_FULL も含む）: fetch_status は据え置き、last_synced だけ更新
    updateLastSyncedOnly();
    return { result: '確認不可', message: 'searchMlitPermit_ error: ' + e.message };
  }

  // 候補 0 件: NOT_FOUND_AT_REFRESH（候補から除外しないので次回も対象）
  if (!candidates || candidates.length === 0) {
    var freshRow1 = resolveMlitPermitRow_(authority, permitNumber);
    if (freshRow1 < 0) return { result: '確認不可', message: '更新時に行が見つからない（手動削除？）' };
    updateRecord_(SHEETS.MLITPermits, freshRow1, {
      fetch_status: 'NOT_FOUND_AT_REFRESH',
      last_synced: nowStr
    });
    return { result: '不一致', message: 'NOT_FOUND_AT_REFRESH' };
  }

  // 詳細 API
  var detail;
  try {
    detail = withMlitRateLimit_(function() {
      return fetchMlitDetail_(candidates[0]);
    }, { maxWaitMs: ROLLING_MAX_WAIT_MS });
  } catch (e) {
    // 一時障害扱い（QUEUE_FULL も含む）
    updateLastSyncedOnly();
    return { result: '確認不可', message: 'fetchMlitDetail_ error: ' + e.message };
  }

  if (!detail || !detail.found) {
    // パース失敗 = 一時障害扱い（次回 stale で再試行）
    updateLastSyncedOnly();
    return { result: '確認不可', message: detail && detail.error ? detail.error : 'detail not found' };
  }

  // 成功: 全フィールド更新（行を再解決してから書き込む）
  var hasIppan = detail.tradesIppan && detail.tradesIppan.length > 0;
  var hasTokutei = detail.tradesTokutei && detail.tradesTokutei.length > 0;
  var category = '';
  if (hasIppan && hasTokutei) category = '般特';
  else if (hasIppan) category = '般';
  else if (hasTokutei) category = '特';

  var allTradesMap = {};
  (detail.tradesIppan || []).forEach(function(t) { if (t) allTradesMap[t] = true; });
  (detail.tradesTokutei || []).forEach(function(t) { if (t) allTradesMap[t] = true; });
  var tradesCount = Object.keys(allTradesMap).length;

  var daysRemaining = detail.expiryTo
    ? Math.floor((new Date(detail.expiryTo) - new Date()) / 86400000)
    : null;

  var freshRow2 = resolveMlitPermitRow_(authority, permitNumber);
  if (freshRow2 < 0) {
    return { result: '確認不可', message: '更新時に行が見つからない（手動削除？）' };
  }
  updateRecord_(SHEETS.MLITPermits, freshRow2, {
    expiry_date: detail.expiryTo || '',
    expiry_wareki: detail.expiryWareki || '',
    days_remaining: daysRemaining !== null ? daysRemaining : '',
    trades_ippan: (detail.tradesIppan || []).join('|'),
    trades_tokutei: (detail.tradesTokutei || []).join('|'),
    trades_count: tradesCount,
    category: category,
    fetch_status: 'OK',
    last_synced: nowStr
  });

  return { result: '一致', message: 'updated' };
}

// ---------------------------------------------------------------------------
// メインエントリ（time-driven trigger 用）
// ---------------------------------------------------------------------------

/**
 * 毎日のローリング再確認バッチ。time-driven trigger から呼ぶ。
 *
 * 設定値（ScriptProperties で上書き可）:
 *   - MLIT_ROLLING_DAILY_LIMIT (default: 5)
 *   - MLIT_ROLLING_MAX_STALE_DAYS (default: 30)
 *   - MLIT_ROLLING_PAUSE='true' で即時停止
 *
 * Note: ScriptLock は使わない（withMlitRateLimit_ が短時間ロック取るのみ）。
 *       多重起動防止は trigger 側設計+冪等性に依存。
 */
function runDailyMlitRolling() {
  if (isMlitRollingPaused_()) {
    Logger.log('runDailyMlitRolling: kill switch ON のため停止');
    return;
  }

  var props = PropertiesService.getScriptProperties();
  var dailyLimit = parseInt(
    props.getProperty('MLIT_ROLLING_DAILY_LIMIT') || MLIT_ROLLING_DEFAULTS_.DAILY_LIMIT,
    10
  );
  var maxStaleDays = parseInt(
    props.getProperty('MLIT_ROLLING_MAX_STALE_DAYS') || MLIT_ROLLING_DEFAULTS_.MAX_STALE_DAYS,
    10
  );

  var candidates = pickMlitRollingCandidates_(dailyLimit, maxStaleDays);
  Logger.log('runDailyMlitRolling: 候補 ' + candidates.length + ' 件 (limit=' + dailyLimit + ', stale=' + maxStaleDays + 'days)');

  var counts = { '一致': 0, '不一致': 0, '確認不可': 0 };
  var startTs = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');

  for (var i = 0; i < candidates.length; i++) {
    if (isMlitRollingPaused_()) {
      Logger.log('runDailyMlitRolling: ループ内で kill switch を検出、' + i + '/' + candidates.length + ' で中断');
      break;
    }

    var permit = candidates[i];
    try {
      var res = refreshOneMlitPermit_(permit);
      counts[res.result] = (counts[res.result] || 0) + 1;
      Logger.log(
        '[' + (i + 1) + '/' + candidates.length + '] ' +
        permit.company_id + ' ' + permit.authority + ' ' + permit.permit_number +
        ' → ' + res.result + ' (' + res.message + ')'
      );
    } catch (err) {
      counts['確認不可']++;
      Logger.log('refreshOneMlitPermit_ エラー (company_id=' + permit.company_id + '): ' + err.message);
    }
    // 待機は withMlitRateLimit_ 内で処理されるため、ここでは sleep しない
  }

  writeAuditLog_(
    'system',
    'MLIT_ROLLING',
    'Batch',
    'daily',
    'started=' + startTs +
    ' limit=' + dailyLimit +
    ' picked=' + candidates.length +
    ' 一致=' + counts['一致'] +
    ' 不一致=' + counts['不一致'] +
    ' 確認不可=' + counts['確認不可']
  );

  Logger.log(
    'runDailyMlitRolling 完了: 一致=' + counts['一致'] +
    ', 不一致=' + counts['不一致'] +
    ', 確認不可=' + counts['確認不可']
  );
}

// ---------------------------------------------------------------------------
// テスト・運用補助
// ---------------------------------------------------------------------------

/**
 * 候補選択だけを試す（GAS エディタから手動実行）
 */
function debugPickMlitRollingCandidates() {
  var candidates = pickMlitRollingCandidates_(
    MLIT_ROLLING_DEFAULTS_.DAILY_LIMIT,
    MLIT_ROLLING_DEFAULTS_.MAX_STALE_DAYS
  );
  Logger.log('候補数: ' + candidates.length);
  candidates.forEach(function(c, i) {
    Logger.log(
      (i + 1) + ': ' + c.company_id + ' ' + c.authority + ' ' + c.permit_number +
      ' last_synced=' + c.last_synced +
      ' fetch_status=' + c.fetch_status
    );
  });
}

/**
 * 1件だけ手動で再確認（GAS エディタから company_id 指定で実行）
 * @param {string} companyId
 */
function debugRefreshOneByCompanyId(companyId) {
  var permit = findByKey_(SHEETS.MLITPermits, 'company_id', companyId);
  if (!permit) {
    Logger.log('company_id=' + companyId + ' の MLITPermits 行が見つかりません');
    return;
  }
  var res = refreshOneMlitPermit_(permit);
  Logger.log('result: ' + res.result + ' message: ' + res.message);
}
