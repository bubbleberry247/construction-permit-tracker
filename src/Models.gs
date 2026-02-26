/**
 * Models.gs — スプレッドシートへのデータアクセス層
 * Companies / Permits / Submissions / Notifications
 */

var HEADER_ROW = 1;
var DATA_START_ROW = 2;

// ─────────────────────────────────────────────────
// 内部ヘルパー
// ─────────────────────────────────────────────────

/**
 * シートの全データを [{header: value, ...}] 形式で返す
 * @param {Sheet} sheet
 * @return {Array<{row: number, data: Object}>}
 */
function sheetToObjects_(sheet) {
  var allValues = sheet.getDataRange().getValues();
  if (allValues.length < DATA_START_ROW) return [];
  var headers = allValues[HEADER_ROW - 1];
  var result = [];
  for (var i = DATA_START_ROW - 1; i < allValues.length; i++) {
    var row = allValues[i];
    var obj = { _row: i + 1 };
    headers.forEach(function(h, idx) {
      obj[h] = row[idx];
    });
    result.push(obj);
  }
  return result;
}

/**
 * オブジェクトをヘッダ順に配列化してシートに追記する
 * @param {Sheet} sheet
 * @param {string[]} headers
 * @param {Object} data
 */
function appendRow_(sheet, headers, data) {
  var row = headers.map(function(h) {
    return data[h] !== undefined ? data[h] : '';
  });
  sheet.appendRow(row);
}

/**
 * 指定行を更新する
 * @param {Sheet} sheet
 * @param {number} rowNum  1-indexed
 * @param {string[]} headers
 * @param {Object} data
 */
function updateRow_(sheet, rowNum, headers, data) {
  var existingRow = sheet.getRange(rowNum, 1, 1, headers.length).getValues()[0];
  var currentObj = {};
  headers.forEach(function(h, idx) { currentObj[h] = existingRow[idx]; });
  Object.keys(data).forEach(function(k) { currentObj[k] = data[k]; });
  var newRow = headers.map(function(h) { return currentObj[h] !== undefined ? currentObj[h] : ''; });
  sheet.getRange(rowNum, 1, 1, headers.length).setValues([newRow]);
}

// ─────────────────────────────────────────────────
// CompaniesModel
// ─────────────────────────────────────────────────

var COMPANIES_HEADERS = [
  'company_id', 'company_name', 'representative_name', 'contact_person',
  'contact_email', 'contact_email_cc', 'phone', 'status', 'created_at', 'updated_at'
];

var CompaniesModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Companies');
    if (!sheet) throw new Error('Companiesシートが見つかりません');
    return sheet;
  },

  findByName: function(companyName) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var lowerName = String(companyName).toLowerCase();
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].company_name).toLowerCase().indexOf(lowerName) !== -1) {
        return rows[i];
      }
    }
    return null;
  },

  findByNameAndEmail: function(companyName, contactEmail) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var normName = String(companyName).trim().toLowerCase();
    var normEmail = String(contactEmail).trim().toLowerCase();
    for (var i = 0; i < rows.length; i++) {
      if (
        String(rows[i].company_name).trim().toLowerCase() === normName &&
        String(rows[i].contact_email).trim().toLowerCase() === normEmail
      ) {
        return rows[i];
      }
    }
    return null;
  },

  findById: function(companyId) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].company_id) === String(companyId)) {
        return rows[i];
      }
    }
    return null;
  },

  create: function(data) {
    var sheet = this.getSheet();
    var now = new Date();
    data.company_id = data.company_id || generateUuid();
    data.created_at = now;
    data.updated_at = now;
    appendRow_(sheet, COMPANIES_HEADERS, data);
    return data;
  },

  update: function(companyId, data) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].company_id) === String(companyId)) {
        data.updated_at = new Date();
        updateRow_(sheet, rows[i]._row, COMPANIES_HEADERS, data);
        return true;
      }
    }
    return false;
  }
};

// ─────────────────────────────────────────────────
// PermitsModel
// ─────────────────────────────────────────────────

var PERMITS_HEADERS = [
  'permit_id', 'company_id', 'permit_number', 'governor_or_minister',
  'general_or_specific', 'permit_type_code', 'trade_categories',
  'issue_date', 'expiry_date', 'renewal_deadline_date', 'status',
  'last_received_date', 'last_checked_date', 'evidence_renewal_application',
  'evidence_file_url', 'permit_file_url', 'permit_file_drive_id',
  'permit_file_version', 'note', 'created_at', 'updated_at'
];

var PermitsModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Permits');
    if (!sheet) throw new Error('Permitsシートが見つかりません');
    return sheet;
  },

  findByCompanyAndNumber: function(companyId, permitNumber) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var normNumber = String(permitNumber).trim();
    for (var i = 0; i < rows.length; i++) {
      if (
        String(rows[i].company_id) === String(companyId) &&
        String(rows[i].permit_number).trim() === normNumber
      ) {
        return rows[i];
      }
    }
    return null;
  },

  create: function(data) {
    var sheet = this.getSheet();
    var now = new Date();
    data.permit_id = data.permit_id || generateUuid();
    data.permit_file_version = data.permit_file_version || 1;
    data.created_at = now;
    data.updated_at = now;
    appendRow_(sheet, PERMITS_HEADERS, data);
    return data;
  },

  update: function(permitId, data) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].permit_id) === String(permitId)) {
        data.updated_at = new Date();
        updateRow_(sheet, rows[i]._row, PERMITS_HEADERS, data);
        return true;
      }
    }
    return false;
  },

  getAllActive: function() {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    return rows.filter(function(r) {
      return String(r.status).toUpperCase() !== 'EXPIRED';
    });
  }
};

// ─────────────────────────────────────────────────
// SubmissionsModel
// ─────────────────────────────────────────────────

var SUBMISSIONS_HEADERS = [
  'submission_id', 'trigger_uid', 'submitted_at', 'company_name_raw', 'contact_email_raw',
  'permit_number_raw', 'expiry_date_raw', 'uploaded_file_drive_id',
  'uploaded_file_url', 'parsed_result', 'error_message'
];

var SubmissionsModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Submissions');
    if (!sheet) throw new Error('Submissionsシートが見つかりません');
    return sheet;
  },

  create: function(data) {
    var sheet = this.getSheet();
    data.submission_id = data.submission_id || generateUuid();
    data.submitted_at = data.submitted_at || new Date();
    appendRow_(sheet, SUBMISSIONS_HEADERS, data);
    return data;
  },

  findByTriggerUid: function(triggerUid) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].trigger_uid) === String(triggerUid)) {
        return rows[i];
      }
    }
    return null;
  },

  /**
   * 指定 submission_id の行を更新する（parsed_result/error_message修正用）
   */
  updateById: function(submissionId, data) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].submission_id) === String(submissionId)) {
        updateRow_(sheet, rows[i]._row, SUBMISSIONS_HEADERS, data);
        return true;
      }
    }
    return false;
  }
};

// ─────────────────────────────────────────────────
// NotificationsModel
// ─────────────────────────────────────────────────

var NOTIFICATIONS_HEADERS = [
  'notification_id', 'sent_at', 'company_id', 'permit_id',
  'to_email', 'cc_email', 'stage', 'subject', 'body', 'result', 'error_message'
];

var NotificationsModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Notifications');
    if (!sheet) throw new Error('Notificationsシートが見つかりません');
    return sheet;
  },

  create: function(data) {
    var sheet = this.getSheet();
    data.notification_id = data.notification_id || generateUuid();
    data.sent_at = data.sent_at || new Date();
    appendRow_(sheet, NOTIFICATIONS_HEADERS, data);
    return data;
  },

  hasBeenSent: function(permitId, stage) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      var result = String(rows[i].result);
      if (
        String(rows[i].permit_id) === String(permitId) &&
        String(rows[i].stage) === String(stage) &&
        (result === 'SENT' || result === 'PENDING')
      ) {
        return true;
      }
    }
    return false;
  },

  /**
   * notification_id で行を検索し、指定カラムを更新する
   * @param {string} notificationId
   * @param {Object} data  更新するカラムのオブジェクト（例: { result: 'SENT' }）
   * @return {boolean} 更新成功なら true
   */
  updateById: function(notificationId, data) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      if (String(rows[i].notification_id) === String(notificationId)) {
        updateRow_(sheet, rows[i]._row, NOTIFICATIONS_HEADERS, data);
        return true;
      }
    }
    return false;
  },

  /**
   * 今日の SENT 件数を返す（Gmail送信制限チェック用）
   * @return {number}
   */
  countSentToday: function() {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var today = formatDate(new Date(), 'yyyy/MM/dd');
    var count = 0;
    rows.forEach(function(r) {
      if (String(r.result) === 'SENT') {
        var sentAt = r.sent_at;
        var sentDate = formatDate(sentAt instanceof Date ? sentAt : new Date(sentAt), 'yyyy/MM/dd');
        if (sentDate === today) count++;
      }
    });
    return count;
  }
};
