/**
 * MasterService.gs — 会社マスタの安全な閲覧・連絡先即時更新・取消
 */

var COMPANY_CONTACT_FIELDS_ = [
  'representative_name',
  'contact_person',
  'contact_email',
  'contact_email_cc',
  'phone',
  'internal_owner_email'
];

var COMPANY_REASON_CODES_ = [
  'CUSTOMER_REQUEST',
  'PERIODIC_REVIEW',
  'CORRECTION',
  'OTHER'
];

function normalizeTextInput_(value, maxLength, label) {
  var text = String(value === null || value === undefined ? '' : value).trim();
  if (text.length > maxLength) {
    throw appError_('INVALID_FIELD', label + 'は' + maxLength + '文字以内で入力してください', false);
  }
  if (/[\u0000-\u001f\u007f]/.test(text)) {
    throw appError_('INVALID_FIELD', label + 'に制御文字は使用できません', false);
  }
  return text;
}

function normalizeCcEmails_(value) {
  var rawItems = Array.isArray(value) ? value : String(value || '').split(',');
  var unique = {};
  var emails = [];
  rawItems.forEach(function(item) {
    var email = normalizeEmailAddress_(item);
    if (!email) return;
    if (!isValidEmailSyntax_(email)) {
      throw appError_('INVALID_EMAIL', 'CCメールアドレスが不正です: ' + email, false);
    }
    if (!unique[email]) {
      unique[email] = true;
      emails.push(email);
    }
  });
  if (emails.length > 5) {
    throw appError_('INVALID_EMAIL', 'CCは最大5件です', false);
  }
  return emails.join(',');
}

function normalizeCompanyContactChanges_(changes) {
  assertOnlyKeys_(changes, COMPANY_CONTACT_FIELDS_, 'changes');
  var normalized = {};
  Object.keys(changes).forEach(function(field) {
    var value = changes[field];
    if (field === 'contact_email') {
      var email = normalizeEmailAddress_(value);
      if (email && !isValidEmailSyntax_(email)) {
        throw appError_('INVALID_EMAIL', 'Toメールアドレスが不正です', false);
      }
      normalized[field] = email;
    } else if (field === 'contact_email_cc') {
      normalized[field] = normalizeCcEmails_(value);
    } else if (field === 'phone') {
      normalized[field] = normalizeTextInput_(value, 30, '電話番号');
    } else if (field === 'internal_owner_email') {
      var ownerEmail = normalizeEmailAddress_(value);
      if (ownerEmail) {
        if (!isValidEmailSyntax_(ownerEmail)) {
          throw appError_('INVALID_OWNER', '社内担当者メールアドレスが不正です', false);
        }
        var owner = getUserAccessByEmail_(ownerEmail);
        if (!owner || !owner.active) {
          throw appError_('INVALID_OWNER', '有効な社内担当者を選択してください', false);
        }
      }
      normalized[field] = ownerEmail;
    } else {
      normalized[field] = normalizeTextInput_(value, 100, field);
    }
  });
  return normalized;
}

function getCompanyVersion_(company) {
  var version = Number(company && company.data_version);
  return Number.isInteger(version) && version > 0 ? version : 1;
}

function getCompanyReadiness_(company) {
  var email = normalizeEmailAddress_(company.contact_email);
  if (!email) return { code: 'EMAIL_MISSING', label: 'メール未設定', sendReady: false };
  if (!isValidEmailSyntax_(email)) {
    return { code: 'EMAIL_INVALID', label: 'メール形式要確認', sendReady: false };
  }
  if (!company.contact_verified_at || !company.contact_verified_by) {
    return { code: 'CONTACT_UNVERIFIED', label: '連絡先未確認', sendReady: false };
  }
  if (String(company.status || 'ACTIVE').toUpperCase() !== 'ACTIVE') {
    return { code: 'INACTIVE', label: '利用停止', sendReady: false };
  }
  return { code: 'READY', label: '送信準備完了', sendReady: true };
}

function serializeCompanyForClient_(company) {
  if (!company) return null;
  var ccForDisplay = String(company.contact_email_cc || '').trim();
  try {
    ccForDisplay = normalizeCcEmails_(ccForDisplay);
  } catch (ignoreInvalidLegacyCc) {
    // 既存データの不備で一覧全体を読めなくしない。保存時には厳格検証する。
  }
  return {
    company_id: String(company.company_id || ''),
    vendor_no: String(company.vendor_no || ''),
    company_name_raw: String(company.company_name_raw || ''),
    company_name_normalized: String(company.company_name_normalized || ''),
    representative_name: String(company.representative_name || ''),
    contact_person: String(company.contact_person || ''),
    contact_email: normalizeEmailAddress_(company.contact_email),
    contact_email_cc: ccForDisplay,
    phone: String(company.phone || ''),
    internal_owner_email: normalizeEmailAddress_(company.internal_owner_email),
    contact_verified_at: company.contact_verified_at || '',
    contact_verified_by: normalizeEmailAddress_(company.contact_verified_by),
    notification_mode: String(company.notification_mode || 'MANUAL'),
    status: String(company.status || 'ACTIVE'),
    data_version: getCompanyVersion_(company),
    created_at: company.created_at || '',
    created_by: normalizeEmailAddress_(company.created_by),
    updated_at: company.updated_at || '',
    updated_by: normalizeEmailAddress_(company.updated_by),
    readiness: getCompanyReadiness_(company)
  };
}

function listCompaniesSecure_(payload) {
  assertOnlyKeys_(payload, ['query', 'pageToken', 'limit'], 'companies.list');
  var query = normalizeTextInput_(payload.query || '', 100, '検索条件').toLowerCase();
  var limit = payload.limit === undefined ? 50 : Number(payload.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
    throw appError_('INVALID_LIMIT', 'limitは1〜100で指定してください', false);
  }
  var offset = payload.pageToken ? Number(payload.pageToken) : 0;
  if (!Number.isInteger(offset) || offset < 0) {
    throw appError_('INVALID_PAGE_TOKEN', 'pageTokenが不正です', false);
  }

  var companies = readRecords_(SHEETS.Companies).filter(function(company) {
    if (!query) return true;
    var haystack = [
      company.company_id,
      company.vendor_no,
      company.company_name_raw,
      company.company_name_normalized
    ].join(' ').toLowerCase();
    return haystack.indexOf(query) >= 0;
  });
  companies.sort(function(a, b) {
    var nameA = String(a.company_name_normalized || a.company_name_raw || '');
    var nameB = String(b.company_name_normalized || b.company_name_raw || '');
    return nameA.localeCompare(nameB, 'ja');
  });

  var page = companies.slice(offset, offset + limit).map(serializeCompanyForClient_);
  return {
    items: page,
    total: companies.length,
    nextPageToken: offset + limit < companies.length ? String(offset + limit) : ''
  };
}

function getCompanySecure_(payload) {
  assertOnlyKeys_(payload, ['companyId'], 'companies.get');
  var companyId = normalizeTextInput_(payload.companyId, 50, '会社ID');
  if (!companyId) throw appError_('COMPANY_ID_REQUIRED', '会社IDが必要です', false);
  var company = findByKey_(SHEETS.Companies, 'company_id', companyId);
  if (!company) throw appError_('NOT_FOUND', '会社が見つかりません', false);

  var detail = getCompanyDetail_(companyId);
  return {
    company: serializeCompanyForClient_(company),
    permits: detail.permits || [],
    notifications: (detail.notifications || []).slice(0, 20),
    auditLog: (detail.auditLog || []).slice(0, 20),
    internalOwners: readRecords_(SHEETS.UserAccess).filter(function(record) {
      return parseStrictBoolean_(record.active);
    }).map(function(record) {
      return {
        email: normalizeEmailAddress_(record.email),
        displayName: String(record.displayName || '')
      };
    })
  };
}

function findCommittedAuditByRequest_(requestId, action) {
  var records = readRecords_(SHEETS.AuditLog);
  for (var i = records.length - 1; i >= 0; i--) {
    if (String(records[i].request_id || '') === String(requestId) &&
        String(records[i].action || '') === String(action) &&
        String(records[i].status || '') === 'COMMITTED') {
      return records[i];
    }
  }
  return null;
}

function markCompanyQueueItemsStale_(companyId, currentDataVersion) {
  var sheet = getSheet_(SHEETS.NotificationQueue);
  ensureHeaders_(sheet, NOTIFICATION_QUEUE_HEADERS);
  var rows = readRecords_(SHEETS.NotificationQueue);
  var changed = 0;
  rows.forEach(function(queue) {
    var status = String(queue.status || '');
    if (String(queue.company_id || '') === String(companyId) &&
        ['DRAFT', 'READY', 'APPROVED'].indexOf(status) >= 0 &&
        Number(queue.source_data_version || 0) !== Number(currentDataVersion)) {
      updateRecord_(SHEETS.NotificationQueue, queue._row, {
        status: 'STALE',
        updated_at: getNowString_(),
        error_code: 'MASTER_VERSION_CHANGED',
        error_message: '会社マスタ更新のため再生成が必要です'
      });
      changed++;
    }
  });
  return changed;
}

function validateChangeReason_(payload) {
  var code = String(payload.reasonCode || '').trim().toUpperCase();
  var note = normalizeTextInput_(payload.reasonNote || '', 200, '変更理由補足');
  if (COMPANY_REASON_CODES_.indexOf(code) < 0) {
    throw appError_('INVALID_REASON', '変更理由を選択してください', false);
  }
  if (code === 'OTHER' && !note) {
    throw appError_('INVALID_REASON', 'OTHERの場合は理由補足が必要です', false);
  }
  return { code: code, note: note };
}

function updateCompanyContactsSecure_(payload, user, requestId) {
  assertOnlyKeys_(
    payload,
    ['companyId', 'dataVersion', 'changes', 'reasonCode', 'reasonNote', 'verifyContact'],
    'companies.updateContacts'
  );
  var companyId = normalizeTextInput_(payload.companyId, 50, '会社ID');
  var expectedVersion = Number(payload.dataVersion);
  if (!companyId) throw appError_('COMPANY_ID_REQUIRED', '会社IDが必要です', false);
  if (!Number.isInteger(expectedVersion) || expectedVersion < 1) {
    throw appError_('INVALID_VERSION', 'dataVersionが不正です', false);
  }
  if (typeof payload.verifyContact !== 'boolean') {
    throw appError_('INVALID_PAYLOAD', 'verifyContactはbooleanで指定してください', false);
  }
  var changes = normalizeCompanyContactChanges_(payload.changes || {});
  var reason = validateChangeReason_(payload);

  var replay = findCommittedAuditByRequest_(requestId, 'UPDATE_COMPANY_CONTACTS');
  if (replay) {
    var replayCompany = findByKey_(SHEETS.Companies, 'company_id', companyId);
    return {
      company: serializeCompanyForClient_(replayCompany),
      auditId: replay.log_id,
      idempotentReplay: true
    };
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', '会社マスタ更新が混雑しています', true);
  }
  var preparedAudit = null;
  try {
    ensureHeaders_(getSheet_(SHEETS.Companies), SECURE_COMPANIES_HEADERS_);
    var existing = findByKey_(SHEETS.Companies, 'company_id', companyId);
    if (!existing) throw appError_('NOT_FOUND', '会社が見つかりません', false);
    var currentVersion = getCompanyVersion_(existing);
    if (currentVersion !== expectedVersion) {
      throw appError_('STALE_VERSION', '他の担当者が先に更新しました', true);
    }

    var before = serializeCompanyForClient_(existing);
    var updates = {};
    Object.keys(changes).forEach(function(field) {
      if (String(existing[field] || '') !== String(changes[field] || '')) {
        updates[field] = changes[field];
      }
    });

    var effectiveEmail = updates.contact_email !== undefined
      ? updates.contact_email
      : normalizeEmailAddress_(existing.contact_email);
    if (payload.verifyContact && !effectiveEmail) {
      throw appError_('CONTACT_EMAIL_REQUIRED', '確認済みにするにはToメールが必要です', false);
    }
    if (payload.verifyContact) {
      updates.contact_verified_at = getNowString_();
      updates.contact_verified_by = user.email;
    } else if (updates.contact_email !== undefined ||
               updates.contact_email_cc !== undefined ||
               existing.contact_verified_at) {
      updates.contact_verified_at = '';
      updates.contact_verified_by = '';
    }

    updates.data_version = currentVersion + 1;
    updates.updated_at = getNowString_();
    updates.updated_by = user.email;
    var after = Object.assign({}, before, updates, {
      readiness: undefined
    });

    preparedAudit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'UPDATE_COMPANY_CONTACTS',
      target_type: 'Company',
      target_id: companyId,
      request_id: requestId,
      reason_code: reason.code,
      reason_note: reason.note,
      before_json: JSON.stringify(before),
      after_json: JSON.stringify(after),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();

    try {
      updateRecord_(SHEETS.Companies, existing._row, updates);
      SpreadsheetApp.flush();
    } catch (updateError) {
      var abortRow = findByKey_(SHEETS.AuditLog, 'log_id', preparedAudit.log_id);
      if (abortRow) {
        updateRecord_(SHEETS.AuditLog, abortRow._row, {
          status: 'ABORTED',
          error_code: 'MASTER_WRITE_FAILED',
          details: String(updateError.message || updateError).substring(0, 500)
        });
      }
      throw updateError;
    }

    var staleCount = markCompanyQueueItemsStale_(companyId, updates.data_version);
    var preparedRow = findByKey_(SHEETS.AuditLog, 'log_id', preparedAudit.log_id);
    if (!preparedRow) {
      throw appError_(
        'COMMITTED_AUDIT_PENDING',
        '更新は反映されましたが監査ログ確定が必要です',
        false,
        preparedAudit.log_id
      );
    }
    updateRecord_(SHEETS.AuditLog, preparedRow._row, {
      status: 'COMMITTED',
      details: JSON.stringify({ staleQueueItems: staleCount })
    });
    SpreadsheetApp.flush();

    var current = findByKey_(SHEETS.Companies, 'company_id', companyId);
    return {
      company: serializeCompanyForClient_(current),
      auditId: preparedAudit.log_id,
      staleQueueItems: staleCount,
      idempotentReplay: false
    };
  } finally {
    lock.releaseLock();
  }
}

function revertCompanyChangeSecure_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['auditId', 'reason'], 'companies.revertChange');
  var auditId = normalizeTextInput_(payload.auditId, 100, '監査ID');
  var reason = normalizeTextInput_(payload.reason, 200, '取消理由');
  if (!auditId || !reason) {
    throw appError_('INVALID_PAYLOAD', '監査IDと取消理由が必要です', false);
  }
  var original = findByKey_(SHEETS.AuditLog, 'log_id', auditId);
  if (!original ||
      String(original.action || '') !== 'UPDATE_COMPANY_CONTACTS' ||
      String(original.status || '') !== 'COMMITTED') {
    throw appError_('NOT_REVERSIBLE', '取消可能な更新が見つかりません', false);
  }
  var before;
  var after;
  try {
    before = JSON.parse(String(original.before_json || '{}'));
    after = JSON.parse(String(original.after_json || '{}'));
  } catch (parseError) {
    throw appError_('NOT_REVERSIBLE', '監査データを復元できません', false);
  }
  var companyId = String(original.target_id || '');

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', '取消処理が混雑しています', true);
  try {
    var current = findByKey_(SHEETS.Companies, 'company_id', companyId);
    if (!current) throw appError_('NOT_FOUND', '会社が見つかりません', false);
    if (getCompanyVersion_(current) !== Number(after.data_version)) {
      throw appError_('STALE_VERSION', '後続の更新があるため取消できません', false);
    }
    var updates = {};
    COMPANY_CONTACT_FIELDS_.forEach(function(field) {
      updates[field] = before[field] || '';
    });
    updates.contact_verified_at = before.contact_verified_at || '';
    updates.contact_verified_by = before.contact_verified_by || '';
    updates.data_version = getCompanyVersion_(current) + 1;
    updates.updated_at = getNowString_();
    updates.updated_by = user.email;
    updateRecord_(SHEETS.Companies, current._row, updates);
    markCompanyQueueItemsStale_(companyId, updates.data_version);

    var audit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'REVERT_COMPANY_CONTACTS',
      target_type: 'Company',
      target_id: companyId,
      request_id: requestId,
      reason_code: 'CORRECTION',
      reason_note: reason,
      before_json: JSON.stringify(serializeCompanyForClient_(current)),
      after_json: JSON.stringify(Object.assign({}, current, updates)),
      details: 'reverted_audit_id=' + auditId,
      status: 'COMMITTED'
    });
    SpreadsheetApp.flush();
    return {
      company: serializeCompanyForClient_(
        findByKey_(SHEETS.Companies, 'company_id', companyId)
      ),
      auditId: audit.log_id
    };
  } finally {
    lock.releaseLock();
  }
}
