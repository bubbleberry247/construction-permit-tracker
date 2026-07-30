/**
 * Schema.gs — 既存列を維持する非破壊schema移行とUserAccess管理
 */

var SECURE_COMPANIES_HEADERS_ = [
  'company_id', 'company_name_raw', 'company_name_normalized',
  'representative_name', 'contact_person', 'contact_email',
  'contact_email_cc', 'phone', 'status', 'created_at', 'updated_at',
  'vendor_no', 'internal_owner_email',
  'contact_verified_at', 'contact_verified_by', 'notification_mode',
  'data_version', 'created_by', 'updated_by'
];

var SECURE_NOTIFICATIONS_HEADERS_ = [
  'notification_id', 'sent_at', 'company_id', 'permit_id',
  'to_email', 'cc_email', 'stage', 'subject', 'body', 'result', 'error_message',
  'bcc_email', 'queue_id', 'idempotency_key', 'initiated_by',
  'send_origin', 'recipient_count', 'notification_mode'
];

var INITIAL_USER_ACCESS_ = [
  {
    email: 'm-fujita@tokai-ic.co.jp',
    role: 'master_editor',
    active: true,
    displayName: '藤田',
    managerEmail: 'kanri.tic@tokai-ic.co.jp',
    canSendExternal: true
  },
  {
    email: 'kanri.tic@tokai-ic.co.jp',
    role: 'operations_admin',
    active: true,
    displayName: '運用管理',
    managerEmail: '',
    canSendExternal: true
  },
  {
    email: 'kalimistk@gmail.com',
    role: 'technical_admin',
    active: true,
    displayName: '技術管理',
    managerEmail: '',
    canSendExternal: false
  }
];

function getNowString_() {
  return Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
}

function ensureSheetByName_(sheetName, headers) {
  var sheet = getSheet_(sheetName);
  return {
    sheet: sheet,
    result: ensureHeaders_(sheet, headers)
  };
}

function backfillCompanySystemFields_() {
  var companies = readRecords_(SHEETS.Companies);
  var changed = 0;
  companies.forEach(function(company) {
    var updates = {};
    var version = Number(company.data_version);
    if (!Number.isInteger(version) || version < 1) updates.data_version = 1;
    if (!String(company.notification_mode || '').trim()) updates.notification_mode = 'MANUAL';
    if (Object.keys(updates).length > 0) {
      updateRecord_(SHEETS.Companies, company._row, updates);
      changed++;
    }
  });
  return changed;
}

function seedInitialUserAccess_(actorEmail) {
  var sheet = getSheet_(SHEETS.UserAccess);
  ensureHeaders_(sheet, USERACCESS_HEADERS);
  var now = getNowString_();
  var changed = [];
  INITIAL_USER_ACCESS_.forEach(function(seed) {
    var existing = getUserAccessByEmail_(seed.email);
    var record = {
      email: seed.email,
      role: seed.role,
      active: seed.active,
      displayName: seed.displayName,
      updatedAt: now,
      managerEmail: seed.managerEmail,
      canSendExternal: seed.canSendExternal,
      updatedBy: actorEmail
    };
    if (existing) {
      updateRecord_(SHEETS.UserAccess, existing._row, record);
    } else {
      appendRecord_(SHEETS.UserAccess, record);
    }
    changed.push(seed.email);
  });
  return changed;
}

function protectSheetForOwner_(sheetName) {
  var sheet = getSheet_(sheetName);
  var description = 'SYSTEM_MANAGED_' + sheetName;
  var protections = sheet.getProtections(SpreadsheetApp.ProtectionType.SHEET);
  var protection = null;
  for (var i = 0; i < protections.length; i++) {
    if (protections[i].getDescription() === description) {
      protection = protections[i];
      break;
    }
  }
  if (!protection) protection = sheet.protect().setDescription(description);
  protection.setWarningOnly(false);
  var editors = protection.getEditors();
  if (editors && editors.length) protection.removeEditors(editors);
  if (protection.canDomainEdit()) protection.setDomainEdit(false);
  return sheetName;
}

function ensureApplicationSchemaSecure_(payload, user, requestId) {
  assertOnlyKeys_(
    payload,
    ['applyInitialRoles', 'backfillCompanies', 'applyProtections', 'confirmation'],
    'operations.ensureSchema'
  );
  var requestedMutation = payload.applyInitialRoles === true ||
    payload.backfillCompanies === true ||
    payload.applyProtections === true;
  if (requestedMutation && String(payload.confirmation || '') !== 'APPLY_SCHEMA_V1') {
    throw appError_('CONFIRMATION_REQUIRED', 'schema適用確認が一致しません', false);
  }

  var results = {};
  results.Companies = ensureSheetByName_(SHEETS.Companies, SECURE_COMPANIES_HEADERS_).result;
  results.Notifications = ensureSheetByName_(
    SHEETS.Notifications, SECURE_NOTIFICATIONS_HEADERS_
  ).result;
  results.NotificationQueue = ensureSheetByName_(
    SHEETS.NotificationQueue, NOTIFICATION_QUEUE_HEADERS
  ).result;
  results.AuditLog = ensureSheetByName_(SHEETS.AuditLog, AUDIT_HEADERS).result;
  results.UserAccess = ensureSheetByName_(SHEETS.UserAccess, USERACCESS_HEADERS).result;
  results.MasterImportStaging = ensureSheetByName_(
    SHEETS.MasterImportStaging, MASTER_IMPORT_STAGING_HEADERS
  ).result;

  if (!getSecureSetting_('NOTIFICATION_MODE')) {
    setSecureSetting_('NOTIFICATION_MODE', 'OFF');
    setSecureSetting_('NOTIFICATION_MODE_CHANGED_AT', getNowString_());
    setSecureSetting_('NOTIFICATION_MODE_CHANGED_BY', user.email);
  }
  if (!getSecureSetting_('ENABLE_SEND')) {
    setSecureSetting_('ENABLE_SEND', 'FALSE');
  }

  var backfilled = payload.backfillCompanies === true ? backfillCompanySystemFields_() : 0;
  var seededUsers = payload.applyInitialRoles === true
    ? seedInitialUserAccess_(user.email)
    : [];
  var protectedSheets = [];
  if (payload.applyProtections === true) {
    [
      SHEETS.Companies,
      SHEETS.Config,
      SHEETS.UserAccess,
      SHEETS.NotificationQueue,
      SHEETS.Notifications,
      SHEETS.AuditLog,
      SHEETS.MasterImportStaging
    ].forEach(function(name) {
      protectedSheets.push(protectSheetForOwner_(name));
    });
  }

  var audit = appendAuditEvent_({
    user_email: user.email,
    actor_role: user.role,
    action: 'ENSURE_SCHEMA',
    target_type: 'Spreadsheet',
    target_id: SpreadsheetApp.getActiveSpreadsheet().getId(),
    request_id: requestId,
    details: JSON.stringify({
      backfilledCompanies: backfilled,
      seededUsers: seededUsers,
      protectedSheets: protectedSheets
    }),
    status: 'COMMITTED'
  });

  return {
    sheets: results,
    backfilledCompanies: backfilled,
    seededUsers: seededUsers,
    protectedSheets: protectedSheets,
    notificationMode: getSecureSetting_('NOTIFICATION_MODE'),
    auditId: audit.log_id
  };
}

function listUsersSecure_(payload) {
  assertOnlyKeys_(payload, [], 'users.list');
  return readRecords_(SHEETS.UserAccess).map(function(record) {
    return {
      email: normalizeEmailAddress_(record.email),
      role: normalizeRole_(record.role),
      active: parseStrictBoolean_(record.active),
      displayName: String(record.displayName || ''),
      managerEmail: normalizeEmailAddress_(record.managerEmail),
      canSendExternal: parseStrictBoolean_(record.canSendExternal),
      updatedAt: record.updatedAt || '',
      updatedBy: normalizeEmailAddress_(record.updatedBy)
    };
  });
}

function countActiveOperationsAdmins_(excludingEmail) {
  var exclude = normalizeEmailAddress_(excludingEmail);
  return readRecords_(SHEETS.UserAccess).filter(function(record) {
    return normalizeEmailAddress_(record.email) !== exclude &&
      parseStrictBoolean_(record.active) &&
      normalizeRole_(record.role) === 'operations_admin';
  }).length;
}

function updateUserSecure_(payload, user, requestId) {
  assertOnlyKeys_(
    payload,
    ['email', 'role', 'active', 'displayName', 'managerEmail', 'canSendExternal'],
    'users.update'
  );
  var email = normalizeEmailAddress_(payload.email);
  var managerEmail = normalizeEmailAddress_(payload.managerEmail);
  var role = String(payload.role || '').trim().toLowerCase();
  if (!isValidEmailSyntax_(email)) {
    throw appError_('INVALID_EMAIL', '利用者メールアドレスが不正です', false);
  }
  if (['master_editor', 'operations_admin', 'technical_admin'].indexOf(role) < 0) {
    throw appError_('INVALID_ROLE', 'roleが不正です', false);
  }
  if (managerEmail && !isValidEmailSyntax_(managerEmail)) {
    throw appError_('INVALID_MANAGER', '上長メールアドレスが不正です', false);
  }
  if (typeof payload.active !== 'boolean' || typeof payload.canSendExternal !== 'boolean') {
    throw appError_('INVALID_PAYLOAD', 'activeとcanSendExternalはbooleanで指定してください', false);
  }
  if (String(payload.displayName || '').length > 100) {
    throw appError_('INVALID_DISPLAY_NAME', '表示名が長すぎます', false);
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', '利用者更新が混雑しています', true);
  }
  try {
    var existing = getUserAccessByEmail_(email);
    if (existing &&
        existing.role === 'operations_admin' &&
        (!payload.active || role !== 'operations_admin') &&
        countActiveOperationsAdmins_(email) === 0) {
      throw appError_('LAST_ADMIN', '最後の運用管理者は無効化・降格できません', false);
    }
    var now = getNowString_();
    var record = {
      email: email,
      role: role,
      active: payload.active,
      displayName: String(payload.displayName || '').trim(),
      updatedAt: now,
      managerEmail: managerEmail,
      canSendExternal: payload.canSendExternal,
      updatedBy: user.email
    };
    if (existing) {
      updateRecord_(SHEETS.UserAccess, existing._row, record);
    } else {
      appendRecord_(SHEETS.UserAccess, record);
    }
    var audit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'UPDATE_USER_ACCESS',
      target_type: 'UserAccess',
      target_id: email,
      request_id: requestId,
      before_json: existing ? JSON.stringify(existing) : '',
      after_json: JSON.stringify(record),
      status: 'COMMITTED'
    });
    return { user: record, auditId: audit.log_id };
  } finally {
    lock.releaseLock();
  }
}
