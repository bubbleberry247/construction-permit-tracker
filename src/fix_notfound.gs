/**
 * fix_notfound.gs — NOT_FOUND 3社の修正（1回実行用）
 *
 * 実行方法: GASエディタから fixNotFoundCompanies() を選択して実行
 *
 * 修正内容:
 *   C0041 (タイト)     → fetch_status: DUPLICATE_DELETE  (チルトC0068の重複)
 *   C0009 (中根一仁)   → fetch_status: PERSON_NAME_DELETE (代表者名誤登録)
 *   C0028 (トーケン)   → 許可番号26325行: PERMIT_CORRECTED_SEE_ROW12
 *                        ※正しい許可番号62352はrow12に既存
 *   C0070 (サムシング) → row39にOK済み、対応不要
 *
 * 実行日: 2026-03-31 (Python gspread で直接適用済み。GAS版は再確認用)
 */

function fixNotFoundCompanies() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var results = [];

  // === 1. タイト (C0041) → DUPLICATE_DELETE ===
  results.push(setFetchStatus_('C0041', 'DUPLICATE_DELETE', 'OCR誤読: チルト→タイト、C0068の重複'));

  // === 2. 中根一仁 (C0009) → PERSON_NAME_DELETE ===
  results.push(setFetchStatus_('C0009', 'PERSON_NAME_DELETE', '代表者名誤認: ナカミツホームテック(C0063)の中根さん'));

  // === 3. トーケン (C0028) permit 26325 → PERMIT_CORRECTED_SEE_ROW12 ===
  results.push(setFetchStatusByPermit_('C0028', '26325', 'PERMIT_CORRECTED_SEE_ROW12', '許可番号誤記: 26325->62352、正しい行はrow12'));

  // === 4. サムシング (C0070) 確認 ===
  results.push(checkCompanyData_('サムシング'));

  Logger.log('=== NOT_FOUND修正結果 ===');
  results.forEach(function(r) { Logger.log(r); });

  return results.join('\n');
}

/**
 * MLITPermits で company_id が一致する行の fetch_status を更新する。
 * 同一 company_id が複数ある場合は NOT_FOUND の行のみ対象とする。
 */
function setFetchStatus_(companyId, newStatus, reason) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('MLITPermits');
  if (!sheet) return '[ERROR] MLITPermits sheet not found';

  var data = sheet.getDataRange().getValues();
  var headers = data[0];
  var idCol = headers.indexOf('company_id');
  var statusCol = headers.indexOf('fetch_status');
  var syncedCol = headers.indexOf('last_synced');
  if (idCol < 0 || statusCol < 0) return '[ERROR] Required columns not found';

  var updated = [];
  for (var r = 1; r < data.length; r++) {
    if (String(data[r][idCol]).trim() !== companyId) continue;
    var oldStatus = data[r][statusCol];
    sheet.getRange(r + 1, statusCol + 1).setValue(newStatus);
    if (syncedCol >= 0) {
      sheet.getRange(r + 1, syncedCol + 1).setValue(
        Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss')
      );
    }
    updated.push('row=' + (r + 1) + ' ' + oldStatus + '->' + newStatus);
  }
  if (updated.length === 0) return '[SKIP] ' + companyId + ': not found in MLITPermits';
  return '[OK] ' + companyId + ' (' + reason + '): ' + updated.join(', ');
}

/**
 * MLITPermits で company_id かつ permit_number が一致する行だけ更新する。
 * (同一 company_id で複数行ある場合の絞り込み用)
 */
function setFetchStatusByPermit_(companyId, permitNumber, newStatus, reason) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('MLITPermits');
  if (!sheet) return '[ERROR] MLITPermits sheet not found';

  var data = sheet.getDataRange().getValues();
  var headers = data[0];
  var idCol = headers.indexOf('company_id');
  var permitCol = headers.indexOf('permit_number');
  var statusCol = headers.indexOf('fetch_status');
  var syncedCol = headers.indexOf('last_synced');
  if (idCol < 0 || permitCol < 0 || statusCol < 0) return '[ERROR] Required columns not found';

  for (var r = 1; r < data.length; r++) {
    if (String(data[r][idCol]).trim() !== companyId) continue;
    if (String(data[r][permitCol]).trim() !== permitNumber) continue;
    var oldStatus = data[r][statusCol];
    sheet.getRange(r + 1, statusCol + 1).setValue(newStatus);
    if (syncedCol >= 0) {
      sheet.getRange(r + 1, syncedCol + 1).setValue(
        Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss')
      );
    }
    return '[OK] ' + companyId + ' permit=' + permitNumber + ' (' + reason + '): row=' + (r + 1) + ' ' + oldStatus + '->' + newStatus;
  }
  return '[SKIP] ' + companyId + ' permit=' + permitNumber + ': not found in MLITPermits';
}

/**
 * 社名部分一致でMLITPermitsを検索してログに表示する（確認用）
 */
function checkCompanyData_(companyName) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('MLITPermits');
  if (!sheet) return '[ERROR] MLITPermits sheet not found';

  var data = sheet.getDataRange().getValues();
  var headers = data[0];
  var nameCol = headers.indexOf('company_name');
  var idCol = headers.indexOf('company_id');
  var statusCol = headers.indexOf('fetch_status');
  if (nameCol < 0) return '[ERROR] company_name column not found';

  var found = [];
  for (var r = 1; r < data.length; r++) {
    var name = String(data[r][nameCol] || '');
    if (name.indexOf(companyName) >= 0) {
      var cid = idCol >= 0 ? String(data[r][idCol]) : '?';
      var status = statusCol >= 0 ? String(data[r][statusCol]) : '?';
      found.push('row=' + (r + 1) + ' id=' + cid + ' name=' + name + ' fetch_status=' + status);
    }
  }
  if (found.length === 0) return '[INFO] ' + companyName + ': not found in MLITPermits';
  return '[INFO] ' + companyName + ': ' + found.join('; ');
}
