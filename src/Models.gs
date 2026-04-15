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
 * スプレッドシート式インジェクション対策
 * 先頭が =, +, -, @ の文字列にはプレフィクス ' を付与
 */
function sanitizeForSheet_(value) {
  if (typeof value !== 'string') return value;
  if (value.length === 0) return value;
  var firstChar = value.charAt(0);
  if (firstChar === '=' || firstChar === '+' || firstChar === '-' || firstChar === '@') {
    return "'" + value;
  }
  return value;
}

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
    return data[h] !== undefined ? sanitizeForSheet_(data[h]) : '';
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
  var newRow = headers.map(function(h) { return currentObj[h] !== undefined ? sanitizeForSheet_(currentObj[h]) : ''; });
  sheet.getRange(rowNum, 1, 1, headers.length).setValues([newRow]);
}

// ─────────────────────────────────────────────────
// 会社名の検索用正規化（Python側 normalize_company_name と統一）
// 法人格は除去しない。略記→正式表記に変換する。
//   ㈱ → 株式会社、（株） → 株式会社
//   ㈲ → 有限会社、（有） → 有限会社
// これにより「株式会社A」と「有限会社A」は別会社として識別される。
// ─────────────────────────────────────────────────
function normalizeCompanyName_(name) {
  if (!name) return '';
  return String(name)
    .replace(/㈱/g, '株式会社')
    .replace(/㈲/g, '有限会社')
    .replace(/（株）/g, '株式会社')
    .replace(/（有）/g, '有限会社')
    .replace(/\s+/g, '')
    .trim()
    .toLowerCase();
}

// ─────────────────────────────────────────────────
// CompaniesModel
// ─────────────────────────────────────────────────

var COMPANIES_HEADERS = [
  'company_id', 'company_name_raw', 'company_name_normalized',
  'representative_name', 'contact_person',
  'contact_email', 'contact_email_cc', 'phone', 'status', 'created_at', 'updated_at'
];

var CompaniesModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Companies');
    if (!sheet) throw new Error('Companiesシートが見つかりません');
    return sheet;
  },

  /**
   * 正規化済み会社名で会社を検索する（完全一致のみ）。
   * 空文字の場合は null を返す（空クエリで全件ヒットするバグを防止）。
   * @param {string} normalizedName  検索する会社名（正規化済み）
   * @return {Object|null}
   */
  findByNormalizedName: function(normalizedName) {
    if (!normalizedName || String(normalizedName).trim() === '') return null;
    var queryStripped = normalizeCompanyName_(normalizedName);
    if (!queryStripped) return null;
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    for (var i = 0; i < rows.length; i++) {
      var storedName = rows[i].company_name_normalized || rows[i].company_name_raw || '';
      var storedStripped = normalizeCompanyName_(storedName);
      if (storedStripped && storedStripped === queryStripped) {
        return rows[i];
      }
    }
    return null;
  },

  findByNameAndEmail: function(name, email) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var qName = String(name).toLowerCase().trim();
    var qEmail = String(email).toLowerCase().trim();
    for (var i = 0; i < rows.length; i++) {
      var sName = String(rows[i].company_name_normalized || rows[i].company_name_raw || '').toLowerCase().trim();
      var sEmail = String(rows[i].contact_email || '').toLowerCase().trim();
      if ((sName === qName || sName.indexOf(qName) !== -1) && sEmail === qEmail) {
        return rows[i];
      }
    }
    return null;
  },

  findByName: function(name) {
    return this.findByNormalizedName(name);
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
    data.company_id = data.company_id || getNextCompanyId_();
    data.status = data.status || 'ACTIVE';
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
  'permit_id', 'company_id', 'company_name_raw',
  'permit_authority_name', 'permit_authority_name_normalized', 'permit_authority_type', 'permit_category',
  'permit_year', 'contractor_number', 'permit_number_full',
  'trade_categories', 'issue_date', 'expiry_date', 'renewal_deadline_date',
  'current_status', 'evidence_renewal_application', 'renewal_application_date',
  'mlit_confirmed_date', 'mlit_confirm_result', 'mlit_screenshot_url',
  'permit_file_path', 'permit_file_share_url', 'permit_file_version', 'evidence_file_path',
  'last_received_date', 'source_file', 'source_file_hash',
  'parse_status', 'error_category', 'error_reason',
  'note', 'created_at', 'updated_at'
];

var PermitsModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Permits');
    if (!sheet) throw new Error('Permitsシートが見つかりません');
    return sheet;
  },

  findByCompanyAndNumber: function(companyId, permitNumberFull) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var normNumber = String(permitNumberFull).trim();
    for (var i = 0; i < rows.length; i++) {
      if (
        String(rows[i].company_id) === String(companyId) &&
        String(rows[i].permit_number_full).trim() === normNumber
      ) {
        return rows[i];
      }
    }
    return null;
  },

  /**
   * upsertキー（company_id + permit_authority_name_normalized + contractor_number + permit_category）で検索
   * @param {string} companyId
   * @param {string} permitAuthorityNameNormalized  正規化済み行政庁名（「愛知県知事」等）
   * @param {string} contractorNumber  業者番号
   * @param {string} permitCategory  「一般」or「特定」
   * @return {Object|null}
   */
  findByUpsertKey: function(companyId, permitAuthorityNameNormalized, contractorNumber, permitCategory) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var normNum = String(contractorNumber).trim();
    var normAuth = String(permitAuthorityNameNormalized).trim();
    var normCat = String(permitCategory).trim();
    for (var i = 0; i < rows.length; i++) {
      if (
        String(rows[i].company_id) === String(companyId) &&
        String(rows[i].permit_authority_name_normalized).trim() === normAuth &&
        String(rows[i].contractor_number).trim() === normNum &&
        String(rows[i].permit_category).trim() === normCat
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
    data.current_status = data.current_status || 'VALID';
    data.evidence_renewal_application = data.evidence_renewal_application || false;
    data.parse_status = data.parse_status || 'OK';
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
      return String(r.current_status).toUpperCase() !== 'EXPIRED';
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
        result === 'SENT'
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

// ─────────────────────────────────────────────────
// DocumentChecklistModel
// ─────────────────────────────────────────────────

var DOCUMENT_CHECKLIST_HEADERS = [
  'check_id', 'company_id', 'submission_date',
  '新規継続取引申請書', '建設業許可証', '決算書前年度', '決算書前々年度',
  '会社案内', '工事経歴書', '取引先一覧表', '労働安全衛生誓約書',
  '資格略字一覧', '労働者名簿一覧表', '個人事業主_青色申告書',
  'source_pdf_url', 'llm_classification_raw', 'created_at', 'updated_at'
];

var DocumentChecklistModel = {
  getSheet: function() {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('DocumentChecklist');
    if (!sheet) throw new Error('DocumentChecklistシートが見つかりません');
    return sheet;
  },

  create: function(data) {
    var sheet = this.getSheet();
    var now = new Date();
    data.check_id = data.check_id || generateUuid();
    data.created_at = now;
    data.updated_at = now;
    appendRow_(sheet, DOCUMENT_CHECKLIST_HEADERS, data);
    return data;
  },

  findByCompanyAndDate: function(companyId, submissionDate) {
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var normDate = String(submissionDate).trim();
    for (var i = 0; i < rows.length; i++) {
      if (
        String(rows[i].company_id) === String(companyId) &&
        String(rows[i].submission_date).trim() === normDate
      ) {
        return rows[i];
      }
    }
    return null;
  },

  getMissingDocuments: function(companyId) {
    var REQUIRED_DOCS = [
      '新規継続取引申請書', '建設業許可証', '決算書前年度', '会社案内',
      '工事経歴書', '取引先一覧表', '労働安全衛生誓約書'
    ];
    var sheet = this.getSheet();
    var rows = sheetToObjects_(sheet);
    var companyRows = rows.filter(function(r) {
      return String(r.company_id) === String(companyId);
    });
    if (companyRows.length === 0) return REQUIRED_DOCS.slice();
    // 最新提出日の行を使用
    companyRows.sort(function(a, b) {
      return new Date(b.submission_date) - new Date(a.submission_date);
    });
    var latest = companyRows[0];
    return REQUIRED_DOCS.filter(function(doc) {
      return !latest[doc] || String(latest[doc]).toUpperCase() !== 'TRUE';
    });
  }
};
