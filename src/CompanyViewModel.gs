/**
 * CompanyViewModel.gs — CompanyView シート自動生成
 * Scheduler.gs の runDailyNotifications() から毎日呼び出す
 */

// ステータス優先度（数値が大きいほど深刻）
// RENEWAL_IN_PROGRESS は「期限内に申請済み＋受付票あり → 発注継続OK」のため監視のみ。
// RENEWAL_OVERDUE（申請漏れ・高リスク）より低い深刻度とする。
var STATUS_PRIORITY = {
  'VALID':               1,
  'DEFICIENT':           2,
  'EXPIRING':            3,
  'RENEWAL_IN_PROGRESS': 4, // 申請済・受付票確認済 → 発注継続OK、経過観察のみ
  'REQUIRES_ACTION':     5, // 会社不明などデータ品質問題 → 要対応
  'RENEWAL_OVERDUE':     6, // 申請期限超過 → 新規申請必要・高リスク
  'EXPIRED':             7  // 許可失効 → 発注停止必須
};

var COMPANY_VIEW_HEADERS = [
  '会社名', '最悪ステータス', '最短満了日', '残日数',
  '許可件数', '要確認件数', '不備件数', '最短更新期限', '担当者'
];

/**
 * CompanyView シートを再生成する
 * 既存シートをクリアして全行を書き直す
 */
function refreshCompanyView() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var sheet = ss.getSheetByName('CompanyView');
  if (!sheet) {
    sheet = ss.insertSheet('CompanyView');
  }

  sheet.clearContents();
  sheet.clearFormats();

  // ヘッダ行を書き込み（太字・固定）
  var headerRange = sheet.getRange(1, 1, 1, COMPANY_VIEW_HEADERS.length);
  headerRange.setValues([COMPANY_VIEW_HEADERS]);
  headerRange.setFontWeight('bold');
  sheet.setFrozenRows(1);

  // 会社ごとにデータ集約
  var grouped = groupPermitsByCompany_();
  var rows = [];
  Object.keys(grouped).forEach(function(companyId) {
    var aggregated = aggregateCompanyRow_(companyId, grouped[companyId]);
    rows.push(aggregated);
  });

  // 残日数 昇順ソート（最も緊急な会社が上）
  rows.sort(function(a, b) { return a['残日数'] - b['残日数']; });

  // データ書き込みと色付け
  rows.forEach(function(row, i) {
    var rowNum = i + 2; // 1行目はヘッダ
    var values = COMPANY_VIEW_HEADERS.map(function(h) { return row[h] !== undefined ? row[h] : ''; });
    sheet.getRange(rowNum, 1, 1, COMPANY_VIEW_HEADERS.length).setValues([values]);
    var color = statusColor_(row['最悪ステータス']);
    sheet.getRange(rowNum, 1, 1, COMPANY_VIEW_HEADERS.length).setBackground(color);
  });
}

/**
 * Permits シートから全データを取得し、company_id でグループ化する
 * @return {Object} { company_id: [permit_row, ...] }
 */
function groupPermitsByCompany_() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Permits');
  if (!sheet) throw new Error('Permitsシートが見つかりません');

  var allValues = sheet.getDataRange().getValues();
  if (allValues.length < 2) return {};

  var headers = allValues[0];
  var grouped = {};

  for (var i = 1; i < allValues.length; i++) {
    var obj = {};
    headers.forEach(function(h, idx) { obj[h] = allValues[i][idx]; });

    var companyId = String(obj['company_id'] || '').trim();
    if (!companyId) continue;

    if (!grouped[companyId]) grouped[companyId] = [];
    grouped[companyId].push(obj);
  }

  return grouped;
}

/**
 * 1社分の許可リストから集約行を生成する
 * @param {string} companyId
 * @param {Array} permits  その会社の全許可行
 * @return {Object} 集約行オブジェクト
 */
function aggregateCompanyRow_(companyId, permits) {
  // 会社名: Companiesシートから取得、なければ最初の許可行の company_name_raw
  var companyName = getCompanyName_(companyId, permits[0]);

  // 最悪ステータス
  var worstStatus = 'VALID';
  permits.forEach(function(p) {
    var s = String(p['current_status'] || 'VALID').trim();
    worstStatus = worseStatus_(worstStatus, s);
  });

  // 最短満了日（残日数が最小の許可を探す）
  var nearestPermit = findNearestExpiryPermit_(permits);
  var nearestExpiry = nearestPermit ? nearestPermit['expiry_date'] : null;
  var daysLeft = nearestPermit ? daysUntil_(nearestPermit['expiry_date']) : 9999;

  // 最短更新期限（最短満了日の許可に紐付く renewal_deadline_date）
  var nearestRenewalDeadline = nearestPermit ? (nearestPermit['renewal_deadline_date'] || '') : '';

  // 許可件数
  var permitCount = permits.length;

  // 要確認件数: parse_status == "REVIEW_NEEDED"
  var reviewCount = permits.filter(function(p) {
    return String(p['parse_status'] || '').trim() === 'REVIEW_NEEDED';
  }).length;

  // 不備件数: current_status == "DEFICIENT" or "REQUIRES_ACTION"
  var deficientCount = permits.filter(function(p) {
    var s = String(p['current_status'] || '').trim();
    return s === 'DEFICIENT' || s === 'REQUIRES_ACTION';
  }).length;

  // 担当者: Companiesシートの contact_person
  var contactPerson = getContactPerson_(companyId);

  return {
    '会社名':       companyName,
    '最悪ステータス': worstStatus,
    '最短満了日':   formatDate(parseDate(nearestExpiry), 'yyyy/MM/dd') || '',
    '残日数':       daysLeft === 9999 ? '' : daysLeft,
    '許可件数':     permitCount,
    '要確認件数':   reviewCount,
    '不備件数':     deficientCount,
    '最短更新期限': formatDate(parseDate(nearestRenewalDeadline), 'yyyy/MM/dd') || '',
    '担当者':       contactPerson
  };
}

/**
 * 満了日が最も近い（残日数が最小）許可を返す
 * 欠損 expiry_date の許可はスキップし、有効なものがなければ null
 * @param {Array} permits
 * @return {Object|null}
 */
function findNearestExpiryPermit_(permits) {
  var nearest = null;
  var minDays = Infinity;

  permits.forEach(function(p) {
    var d = daysUntil_(p['expiry_date']);
    if (isNaN(d)) return; // 日付なしはスキップ
    if (d < minDays) {
      minDays = d;
      nearest = p;
    }
  });

  return nearest;
}

/**
 * 会社名を取得する（Companiesシート優先、なければ company_name_raw）
 * @param {string} companyId
 * @param {Object} firstPermit  フォールバック用
 * @return {string}
 */
function getCompanyName_(companyId, firstPermit) {
  try {
    var company = CompaniesModel.findById(companyId);
    if (company) {
      return String(company['company_name_normalized'] || company['company_name_raw'] || '').trim()
             || String(firstPermit['company_name_raw'] || companyId).trim();
    }
  } catch (e) {
    // Companiesシートが存在しない等の例外は無視してフォールバック
  }
  return String(firstPermit['company_name_raw'] || companyId).trim();
}

/**
 * Companiesシートから contact_person を取得する
 * @param {string} companyId
 * @return {string}
 */
function getContactPerson_(companyId) {
  try {
    var company = CompaniesModel.findById(companyId);
    if (company) return String(company['contact_person'] || '').trim();
  } catch (e) {
    // フォールバック
  }
  return '';
}

/**
 * 2つのステータスのうちより深刻な方を返す
 * @param {string} a
 * @param {string} b
 * @return {string}
 */
function worseStatus_(a, b) {
  var pa = STATUS_PRIORITY[a] || 0;
  var pb = STATUS_PRIORITY[b] || 0;
  return pb > pa ? b : a;
}

/**
 * 今日から date までの日数を返す（過去はマイナス）
 * 欠損・無効日付は NaN を返す
 * @param {string|Date} date
 * @return {number}
 */
function daysUntil_(date) {
  // Utils.gs の daysUntil() を再利用
  return daysUntil(date);
}

/**
 * worst_status に応じた背景色を返す
 * @param {string} status
 * @return {string} hex color
 */
function statusColor_(status) {
  var COLOR_MAP = {
    'EXPIRED':             '#FF0000',
    'RENEWAL_IN_PROGRESS': '#4A90D9',
    'RENEWAL_OVERDUE':     '#FF8C00',
    'REQUIRES_ACTION':     '#9B59B6',
    'EXPIRING':            '#FFD700',
    'DEFICIENT':           '#E8DAEF',
    'VALID':               '#FFFFFF'
  };
  return COLOR_MAP[status] || '#FFFFFF';
}
