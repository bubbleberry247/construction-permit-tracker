/**
 * db.gs — シートCRUD（GAS Webappスキル準拠）
 */

// ---------------------------------------------------------------------------
// シート定義
// ---------------------------------------------------------------------------
var SHEETS = {
  Config: 'Config',
  Companies: 'Companies',
  Permits: 'Permits',
  MLITPermits: 'MLITPermits',
  Notifications: 'Notifications',
  AuditLog: 'AuditLog',
  SyncRuns: 'SyncRuns',
  UserAccess: 'UserAccess',
  AuthLog: 'AuthLog'
};

var AUDIT_HEADERS = ['log_id', 'timestamp', 'user_email', 'action', 'target_type', 'target_id', 'details'];
var USERACCESS_HEADERS = ['email', 'role', 'active', 'displayName', 'updatedAt'];
var AUTHLOG_HEADERS = ['timestamp', 'step', 'detail'];

// ---------------------------------------------------------------------------
// シート取得
// ---------------------------------------------------------------------------
function getSheet_(name) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    if (name === SHEETS.AuditLog) {
      sheet.getRange(1, 1, 1, AUDIT_HEADERS.length).setValues([AUDIT_HEADERS]);
      sheet.getRange(1, 1, 1, AUDIT_HEADERS.length).setFontWeight('bold').setBackground('#e8f0fe');
    } else if (name === SHEETS.UserAccess) {
      sheet.getRange(1, 1, 1, USERACCESS_HEADERS.length).setValues([USERACCESS_HEADERS]);
      sheet.getRange(1, 1, 1, USERACCESS_HEADERS.length).setFontWeight('bold').setBackground('#e8f0fe');
    } else if (name === SHEETS.AuthLog) {
      sheet.getRange(1, 1, 1, AUTHLOG_HEADERS.length).setValues([AUTHLOG_HEADERS]);
      sheet.getRange(1, 1, 1, AUTHLOG_HEADERS.length).setFontWeight('bold').setBackground('#e8f0fe');
    }
  }
  return sheet;
}

// ---------------------------------------------------------------------------
// 汎用CRUD
// ---------------------------------------------------------------------------

/**
 * シートの全データをオブジェクト配列で返す
 * @param {string} sheetName
 * @return {Object[]} [{header: value, _row: rowNum}, ...]
 */
function readRecords_(sheetName) {
  var sheet = getSheet_(sheetName);
  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var result = [];
  for (var i = 1; i < data.length; i++) {
    var obj = { _row: i + 1 };
    for (var j = 0; j < headers.length; j++) {
      obj[String(headers[j]).trim()] = data[i][j];
    }
    result.push(obj);
  }
  return result;
}

/**
 * キーでレコードを検索
 * @param {string} sheetName
 * @param {string} keyField
 * @param {string} keyValue
 * @return {Object|null}
 */
function findByKey_(sheetName, keyField, keyValue) {
  var records = readRecords_(sheetName);
  for (var i = 0; i < records.length; i++) {
    if (String(records[i][keyField]).trim() === String(keyValue).trim()) {
      return records[i];
    }
  }
  return null;
}

/**
 * キーで複数レコードを検索
 */
function findAllByKey_(sheetName, keyField, keyValue) {
  var records = readRecords_(sheetName);
  return records.filter(function(r) {
    return String(r[keyField]).trim() === String(keyValue).trim();
  });
}

/**
 * 行を追加
 * @param {string} sheetName
 * @param {Object} rowObj — {header: value, ...}
 */
function appendRecord_(sheetName, rowObj) {
  var sheet = getSheet_(sheetName);
  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var row = headers.map(function(h) {
    var key = String(h).trim();
    var val = rowObj[key] !== undefined ? rowObj[key] : '';
    return sanitizeForSheet_(val);
  });
  sheet.appendRow(row);
}

/**
 * 行を更新（楽観ロック付き）
 * @param {string} sheetName
 * @param {number} rowNum — 1-indexed
 * @param {Object} updates — {header: value, ...}
 * @param {string} [expectedUpdatedAt] — 楽観ロック用
 */
function updateRecord_(sheetName, rowNum, updates, expectedUpdatedAt) {
  var sheet = getSheet_(sheetName);
  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var currentRow = sheet.getRange(rowNum, 1, 1, headers.length).getValues()[0];

  // 楽観ロック
  if (expectedUpdatedAt) {
    var updatedAtIdx = headers.indexOf('updated_at');
    if (updatedAtIdx >= 0) {
      var currentUpdatedAt = String(currentRow[updatedAtIdx]);
      if (currentUpdatedAt !== expectedUpdatedAt) {
        throw new Error('CONFLICT: レコードが他のユーザーに更新されています。画面をリロードしてください。');
      }
    }
  }

  // 更新
  for (var i = 0; i < headers.length; i++) {
    var key = String(headers[i]).trim();
    if (updates.hasOwnProperty(key)) {
      currentRow[i] = sanitizeForSheet_(updates[key]);
    }
  }
  sheet.getRange(rowNum, 1, 1, headers.length).setValues([currentRow]);
}

/**
 * 数式インジェクション防止
 */
function sanitizeForSheet_(value) {
  if (typeof value !== 'string') return value;
  if (/^[=+\-@]/.test(value)) return "'" + value;
  return value;
}

// ---------------------------------------------------------------------------
// 監査ログ
// ---------------------------------------------------------------------------

/**
 * 監査ログに記録
 */
function writeAuditLog_(userEmail, action, targetType, targetId, details) {
  var sheet = getSheet_(SHEETS.AuditLog);
  var logId = generateUuid();
  var timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  sheet.appendRow([logId, timestamp, userEmail, action, targetType, targetId, details || '']);
}

// ---------------------------------------------------------------------------
// Date シリアライズ（google.script.run はDate を渡せない）
// ---------------------------------------------------------------------------
function toSerializable_(obj) {
  if (obj === null || obj === undefined) return obj;
  if (obj instanceof Date) return Utilities.formatDate(obj, 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  if (Array.isArray(obj)) return obj.map(toSerializable_);
  if (typeof obj === 'object') {
    var result = {};
    for (var k in obj) {
      if (obj.hasOwnProperty(k)) result[k] = toSerializable_(obj[k]);
    }
    return result;
  }
  return obj;
}
