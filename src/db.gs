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
  NotificationQueue: 'NotificationQueue',
  AuditLog: 'AuditLog',
  SyncRuns: 'SyncRuns',
  UserAccess: 'UserAccess',
  AuthLog: 'AuthLog',
  MasterImportStaging: 'MasterImportStaging'
};

// 既存列は順序を変えず、新規列を末尾に追加する。既存データを破壊しないこと。
var AUDIT_HEADERS = [
  'log_id', 'timestamp', 'user_email', 'action', 'target_type', 'target_id', 'details',
  'request_id', 'actor_role', 'reason_code', 'reason_note',
  'before_json', 'after_json', 'status', 'error_code'
];
var USERACCESS_HEADERS = [
  'email', 'role', 'active', 'displayName', 'updatedAt',
  'managerEmail', 'canSendExternal', 'updatedBy'
];
var AUTHLOG_HEADERS = ['timestamp', 'step', 'detail'];
var NOTIFICATION_QUEUE_HEADERS = [
  'queue_id', 'idempotency_key', 'company_id', 'permit_id', 'stage',
  'source_data_version', 'to_email', 'cc_email', 'bcc_email',
  'subject', 'body_template', 'addendum', 'status', 'send_origin',
  'created_at', 'created_by', 'approved_at', 'approved_by',
  'sending_at', 'sent_at', 'cancelled_at', 'updated_at',
  'notification_id', 'error_code', 'error_message'
];
var MASTER_IMPORT_STAGING_HEADERS = [
  'source_row', 'vendor_no', 'company_name_raw', 'company_name_normalized',
  'matched_company_id', 'classification', 'match_method',
  'review_status', 'reviewed_by', 'reviewed_at', 'notes',
  'source_sha256', 'imported_at'
];

// ---------------------------------------------------------------------------
// シート取得
// ---------------------------------------------------------------------------
function getSheet_(name) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    var initialHeaders = null;
    if (name === SHEETS.AuditLog) initialHeaders = AUDIT_HEADERS;
    if (name === SHEETS.UserAccess) initialHeaders = USERACCESS_HEADERS;
    if (name === SHEETS.AuthLog) initialHeaders = AUTHLOG_HEADERS;
    if (name === SHEETS.NotificationQueue) initialHeaders = NOTIFICATION_QUEUE_HEADERS;
    if (name === SHEETS.MasterImportStaging) initialHeaders = MASTER_IMPORT_STAGING_HEADERS;
    if (initialHeaders) {
      sheet.getRange(1, 1, 1, initialHeaders.length).setValues([initialHeaders]);
      sheet.getRange(1, 1, 1, initialHeaders.length)
        .setFontWeight('bold')
        .setBackground('#e8f0fe');
      sheet.setFrozenRows(1);
    }
  }
  return sheet;
}

/**
 * 既存ヘッダーを上書きせず、不足列だけ末尾へ追加する。
 * @param {Sheet} sheet
 * @param {string[]} requiredHeaders
 * @return {{added: string[], headers: string[]}}
 */
function ensureHeaders_(sheet, requiredHeaders) {
  if (!sheet) throw new Error('SCHEMA_SHEET_REQUIRED');
  if (!Array.isArray(requiredHeaders) || requiredHeaders.length === 0) {
    throw new Error('SCHEMA_HEADERS_REQUIRED');
  }

  var lastColumn = Math.max(sheet.getLastColumn(), 1);
  var current = sheet.getRange(1, 1, 1, lastColumn).getValues()[0].map(function(value) {
    return String(value || '').trim();
  });
  if (current.length === 1 && current[0] === '') current = [];

  var seen = {};
  current.forEach(function(header) {
    if (header) seen[header] = true;
  });
  var added = requiredHeaders.filter(function(header) {
    if (!header || seen[header]) return false;
    seen[header] = true;
    return true;
  });

  if (added.length > 0) {
    sheet.getRange(1, current.length + 1, 1, added.length).setValues([added]);
    sheet.getRange(1, current.length + 1, 1, added.length)
      .setFontWeight('bold')
      .setBackground('#e8f0fe');
    SpreadsheetApp.flush();
  }
  return { added: added, headers: current.concat(added) };
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
    return sanitizeDbValueForSheet_(val);
  });
  sheet.appendRow(row);
  return Object.assign({ _row: sheet.getLastRow() }, rowObj);
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
      currentRow[i] = sanitizeDbValueForSheet_(updates[key]);
    }
  }
  sheet.getRange(rowNum, 1, 1, headers.length).setValues([currentRow]);
  return true;
}

/**
 * 数式インジェクション防止
 */
function sanitizeDbValueForSheet_(value) {
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
  return appendAuditEvent_({
    user_email: userEmail,
    action: action,
    target_type: targetType,
    target_id: targetId,
    details: details || '',
    status: 'COMMITTED'
  });
}

/**
 * 拡張監査イベントを非破壊schemaで記録する。
 * @param {Object} event
 * @return {Object}
 */
function appendAuditEvent_(event) {
  event = event || {};
  var sheet = getSheet_(SHEETS.AuditLog);
  ensureHeaders_(sheet, AUDIT_HEADERS);
  var timestamp = event.timestamp || Utilities.formatDate(
    new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss'
  );
  var record = {
    log_id: event.log_id || generateUuid_(),
    timestamp: timestamp,
    user_email: String(event.user_email || '').toLowerCase(),
    action: String(event.action || ''),
    target_type: String(event.target_type || ''),
    target_id: String(event.target_id || ''),
    details: String(event.details || ''),
    request_id: String(event.request_id || ''),
    actor_role: String(event.actor_role || ''),
    reason_code: String(event.reason_code || ''),
    reason_note: String(event.reason_note || ''),
    before_json: String(event.before_json || ''),
    after_json: String(event.after_json || ''),
    status: String(event.status || ''),
    error_code: String(event.error_code || '')
  };
  appendRecord_(SHEETS.AuditLog, record);
  return record;
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
