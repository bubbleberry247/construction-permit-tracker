/**
 * PermitSyncService.gs — 許可情報の正本version、MLIT観測差分、手動反映
 */

var MLIT_SYNC_MODES_ = ['OFF', 'SHADOW', 'MANUAL_APPLY'];
var MLIT_SEND_FRESHNESS_HOURS_ = 24;

function isTrackablePermit_(permit) {
  return !!permit && ['CANCELLED', 'SUPERSEDED', 'DELETED'].indexOf(
    String(permit.current_status || '').toUpperCase()
  ) < 0;
}

function getMlitSyncMode_() {
  var mode = String(getSecureSetting_('MLIT_SYNC_MODE') || 'OFF')
    .trim()
    .toUpperCase();
  return MLIT_SYNC_MODES_.indexOf(mode) >= 0 ? mode : 'OFF';
}

function getPermitDataVersion_(permit) {
  var version = Number(permit && permit.permit_data_version);
  return Number.isInteger(version) && version > 0 ? version : 1;
}

function parseStoredTimestamp_(value) {
  if (value instanceof Date) return value;
  var text = String(value || '').trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text)) {
    text = text.replace(' ', 'T') + '+09:00';
  }
  var parsed = new Date(text);
  return isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeExpiryDateKey_(value) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    return Utilities.formatDate(value, 'Asia/Tokyo', 'yyyy-MM-dd');
  }
  var text = String(value || '').trim();
  var matched = text.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})/);
  if (!matched) return '';
  return matched[1] + '-' +
    String(matched[2]).padStart(2, '0') + '-' +
    String(matched[3]).padStart(2, '0');
}

function parseExpiryDateUtc_(value) {
  var normalized = normalizeExpiryDateKey_(value);
  if (!normalized) return null;
  var parts = normalized.split('-').map(Number);
  var parsed = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2]));
  if (parsed.getUTCFullYear() !== parts[0] ||
      parsed.getUTCMonth() !== parts[1] - 1 ||
      parsed.getUTCDate() !== parts[2]) {
    return null;
  }
  return parsed;
}

function classifyMlitExpiryDifference_(approvedExpiry, observedExpiry) {
  var approved = parseExpiryDateUtc_(approvedExpiry);
  var observed = parseExpiryDateUtc_(observedExpiry);
  if (!observed) {
    return { type: 'INVALID_OBSERVED_DATE', riskFlags: ['INVALID_DATE'] };
  }
  if (!approved) {
    return { type: 'INITIAL_OBSERVATION', riskFlags: ['APPROVED_DATE_MISSING'] };
  }
  var deltaDays = Math.round(
    (observed.getTime() - approved.getTime()) / 86400000
  );
  if (deltaDays === 0) return { type: 'NONE', riskFlags: [] };
  if (deltaDays < 0) {
    return { type: 'SHORTENED', riskFlags: ['EXPIRY_SHORTENED'] };
  }

  var expectedFiveYear = new Date(approved.getTime());
  expectedFiveYear.setUTCFullYear(expectedFiveYear.getUTCFullYear() + 5);
  var deviationDays = Math.abs(
    Math.round((observed.getTime() - expectedFiveYear.getTime()) / 86400000)
  );
  return {
    type: 'EXTENDED',
    riskFlags: deviationDays > 120
      ? ['EXTENSION_OUTSIDE_FIVE_YEAR_RANGE']
      : []
  };
}

function normalizeMlitCompanyIdentity_(value) {
  var text = String(value || '').trim();
  if (!text) return '';
  if (typeof normalizeCompanyName_ === 'function') {
    try {
      return String(normalizeCompanyName_(text) || '').replace(/\s+/g, '');
    } catch (ignoredNormalizationFailure) {
      // 下の保守的な正規化へフォールバックする。
    }
  }
  return text
    .replace(/[\s　]/g, '')
    .replace(/[（(]株[)）]/g, '株式会社')
    .replace(/[（(]有[)）]/g, '有限会社');
}

function getCanonicalPermitAuthority_(permit) {
  return String(
    permit && (permit.permit_authority_name_normalized || permit.permit_authority_name) || ''
  ).trim();
}

function getCanonicalPermitNumber_(permit) {
  var contractorNumber = String(permit && permit.contractor_number || '').trim();
  if (contractorNumber) return normalizePermitNumberForMlit_(contractorNumber);
  if (typeof parsePermitNumber_ === 'function') {
    try {
      var parsed = parsePermitNumber_(permit && permit.permit_number_full);
      if (parsed && parsed.parse_success && parsed.contractor_number) {
        return normalizePermitNumberForMlit_(parsed.contractor_number);
      }
    } catch (ignoredParseFailure) {
      // 識別情報不足として呼び出し元で扱う。
    }
  }
  return '';
}

function findMlitObservationForPermit_(permit) {
  if (!permit) return null;
  var permitId = String(permit.permit_id || '').trim();
  var companyId = String(permit.company_id || '').trim();
  var authority = getCanonicalPermitAuthority_(permit);
  var permitNumber = getCanonicalPermitNumber_(permit);
  var rows = readRecords_(SHEETS.MLITPermits);
  var fallback = null;
  for (var i = 0; i < rows.length; i++) {
    if (permitId && String(rows[i].permit_id || '').trim() === permitId) return rows[i];
    if (!fallback &&
        String(rows[i].company_id || '').trim() === companyId &&
        String(rows[i].authority || '').trim() === authority &&
        normalizePermitNumberForMlit_(rows[i].permit_number) === permitNumber) {
      fallback = rows[i];
    }
  }
  return fallback;
}

function ensureMlitObservationForPermit_(permit, company) {
  var authority = getCanonicalPermitAuthority_(permit);
  var permitNumber = getCanonicalPermitNumber_(permit);
  if (!authority || !permitNumber) {
    throw appError_(
      'MLIT_IDENTIFIER_MISSING',
      'MLIT確認に必要な行政庁または許可番号がありません',
      false
    );
  }
  ensureHeaders_(getSheet_(SHEETS.MLITPermits), MLIT_PERMITS_HEADERS_);
  var existing = findMlitObservationForPermit_(permit);
  if (existing) {
    var linkUpdates = {};
    if (!String(existing.permit_id || '').trim()) linkUpdates.permit_id = permit.permit_id;
    if (!String(existing.company_id || '').trim()) linkUpdates.company_id = permit.company_id;
    if (Object.keys(linkUpdates).length > 0) {
      updateRecord_(SHEETS.MLITPermits, existing._row, linkUpdates);
      return findMlitObservationForPermit_(permit);
    }
    return existing;
  }
  var record = {
    company_id: permit.company_id,
    company_name: company
      ? String(company.company_name_normalized || company.company_name_raw || '')
      : String(permit.company_name_raw || ''),
    permit_number: permitNumber,
    authority: authority,
    category: String(permit.permit_category || ''),
    fetch_status: 'PENDING_INITIAL_SYNC',
    permit_id: permit.permit_id,
    diff_status: 'NONE',
    refresh_priority: 'NEW_TARGET'
  };
  appendRecord_(SHEETS.MLITPermits, record);
  return findMlitObservationForPermit_(permit) || record;
}

function getMlitObservationState_(permit) {
  var observation = findMlitObservationForPermit_(permit);
  if (!observation) {
    return {
      code: 'NOT_SYNCED',
      label: 'MLIT未確認',
      sendFresh: false,
      lastAttemptedAt: '',
      lastSuccessAt: '',
      diffStatus: 'NONE'
    };
  }
  var lastSuccess = parseStoredTimestamp_(observation.last_success_at);
  var ageHours = lastSuccess
    ? Math.floor((Date.now() - lastSuccess.getTime()) / 3600000)
    : null;
  var diffStatus = String(observation.diff_status || 'NONE').toUpperCase();
  var diffType = String(observation.diff_type || '').toUpperCase();
  if (diffStatus === 'PENDING_REVIEW') {
    return {
      code: 'DIFF_PENDING',
      label: 'MLIT期限差分の確認待ち',
      sendFresh: false,
      lastAttemptedAt: observation.last_attempted_at || '',
      lastSuccessAt: observation.last_success_at || '',
      ageHours: ageHours,
      diffStatus: diffStatus,
      observedExpiryDate: observation.observed_expiry_date || observation.expiry_date || ''
    };
  }
  if (['IDENTITY_MISMATCH', 'AMBIGUOUS_MATCH', 'NOT_FOUND'].indexOf(diffType) >= 0) {
    return {
      code: 'MLIT_IDENTITY_UNRESOLVED',
      label: 'MLIT照合先が未確定',
      sendFresh: false,
      lastAttemptedAt: observation.last_attempted_at || '',
      lastSuccessAt: observation.last_success_at || '',
      ageHours: ageHours,
      diffStatus: diffStatus,
      diffType: diffType
    };
  }
  if (!lastSuccess) {
    return {
      code: 'NOT_SYNCED',
      label: 'MLIT成功確認なし',
      sendFresh: false,
      lastAttemptedAt: observation.last_attempted_at || '',
      lastSuccessAt: '',
      diffStatus: diffStatus
    };
  }
  var fresh = ageHours >= 0 && ageHours <= MLIT_SEND_FRESHNESS_HOURS_;
  return {
    code: fresh ? 'FRESH' : 'STALE',
    label: fresh ? 'MLIT確認済み' : 'MLIT確認が古い',
    sendFresh: fresh,
    lastAttemptedAt: observation.last_attempted_at || '',
    lastSuccessAt: observation.last_success_at || '',
    ageHours: ageHours,
    diffStatus: diffStatus,
    observedExpiryDate: observation.observed_expiry_date || observation.expiry_date || '',
    fetchStatus: String(observation.fetch_status || '')
  };
}

function markPermitQueueItemsStale_(permitId, currentPermitVersion) {
  var changed = 0;
  readRecords_(SHEETS.NotificationQueue).forEach(function(queue) {
    if (String(queue.permit_id || '') !== String(permitId || '')) return;
    if (['DRAFT', 'READY', 'APPROVED'].indexOf(String(queue.status || '')) < 0) return;
    if (Number(queue.source_permit_version || 0) === Number(currentPermitVersion)) return;
    updateRecord_(SHEETS.NotificationQueue, queue._row, {
      status: 'STALE',
      updated_at: getNowString_(),
      error_code: 'PERMIT_VERSION_CHANGED',
      error_message: '許可情報更新のため再生成が必要です'
    });
    changed++;
  });
  return changed;
}

function assertPermitFreshForSend_(permit, company) {
  if (!isCompanyPermitMonitoringEnabled_(company)) {
    throw appError_('PERMIT_NOT_MONITORED', '許可管理対象外の会社です', false);
  }
  var state = getMlitObservationState_(permit);
  if (!state.sendFresh) {
    throw appError_(
      state.code === 'DIFF_PENDING' ? 'MLIT_DIFF_PENDING' : 'MLIT_NOT_FRESH',
      state.label,
      true
    );
  }
  return state;
}

function serializeMlitDiff_(observation) {
  var permit = findByKey_(SHEETS.Permits, 'permit_id', observation.permit_id);
  var company = permit
    ? findByKey_(SHEETS.Companies, 'company_id', permit.company_id)
    : findByKey_(SHEETS.Companies, 'company_id', observation.company_id);
  return {
    permitId: String(observation.permit_id || ''),
    companyId: String(observation.company_id || ''),
    companyName: company
      ? String(company.company_name_normalized || company.company_name_raw || '')
      : String(observation.company_name || ''),
    permitNumber: permit
      ? String(permit.permit_number_full || '')
      : String(observation.permit_number || ''),
    authority: String(observation.authority || ''),
    approvedExpiryDate: permit ? permit.expiry_date || '' : '',
    observedExpiryDate: observation.observed_expiry_date || observation.expiry_date || '',
    permitDataVersion: permit ? getPermitDataVersion_(permit) : 0,
    diffStatus: String(observation.diff_status || 'NONE'),
    diffDetectedAt: observation.diff_detected_at || '',
    lastAttemptedAt: observation.last_attempted_at || '',
    lastSuccessAt: observation.last_success_at || '',
    fetchStatus: String(observation.fetch_status || ''),
    lastErrorCode: String(observation.last_error_code || ''),
    lastErrorMessage: String(observation.last_error_message || ''),
    observedCompanyName: String(observation.observed_company_name || ''),
    diffType: String(observation.diff_type || ''),
    diffRiskFlags: String(observation.diff_risk_flags || '')
  };
}

function listMlitDiffsSecure_(payload) {
  assertOnlyKeys_(payload, ['status', 'limit'], 'mlit.listDiffs');
  var status = String(payload.status || 'PENDING_REVIEW').trim().toUpperCase();
  var limit = payload.limit === undefined ? 100 : Number(payload.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
    throw appError_('INVALID_LIMIT', 'limitは1〜200で指定してください', false);
  }
  var rows = readRecords_(SHEETS.MLITPermits).filter(function(row) {
    return !status || String(row.diff_status || 'NONE').toUpperCase() === status;
  });
  rows.sort(function(a, b) {
    return String(a.diff_detected_at || '') < String(b.diff_detected_at || '') ? 1 : -1;
  });
  return {
    mode: getMlitSyncMode_(),
    items: rows.slice(0, limit).map(serializeMlitDiff_)
  };
}

function requestMlitRefreshSecure_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['companyId', 'permitId', 'reason'], 'mlit.requestRefresh');
  var companyId = normalizeTextInput_(payload.companyId, 50, '会社ID');
  var permitId = normalizeTextInput_(payload.permitId, 100, '許可ID');
  var reason = normalizeTextInput_(payload.reason, 200, '確認理由');
  if (!companyId && !permitId) {
    throw appError_('TARGET_REQUIRED', '会社IDまたは許可IDが必要です', false);
  }
  var permits = readRecords_(SHEETS.Permits).filter(function(permit) {
    if (!isTrackablePermit_(permit)) return false;
    if (permitId) return String(permit.permit_id || '') === permitId;
    return String(permit.company_id || '') === companyId;
  });
  if (permits.length === 0) {
    throw appError_('PERMIT_NOT_FOUND', '確認対象の許可情報がありません', false);
  }
  if (permits.length > 10) {
    throw appError_('TOO_MANY_PERMITS', '1回に確認予約できる許可は10件までです', false);
  }
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', 'MLIT確認予約が混雑しています', true);
  try {
    var targets = permits.map(function(permit) {
      if (companyId && String(permit.company_id || '') !== companyId) {
        throw appError_('TARGET_MISMATCH', '会社IDと許可IDが一致しません', false);
      }
      var company = findByKey_(SHEETS.Companies, 'company_id', permit.company_id);
      if (!isCompanyPermitMonitoringEnabled_(company)) {
        throw appError_('PERMIT_NOT_MONITORED', '許可管理対象外の会社です', false);
      }
      if (!getCanonicalPermitAuthority_(permit) || !getCanonicalPermitNumber_(permit)) {
        throw appError_(
          'MLIT_IDENTIFIER_MISSING',
          'MLIT確認に必要な行政庁または許可番号がありません',
          false
        );
      }
      return { permit: permit, company: company };
    });
    var requestedAt = getNowString_();
    var plannedItems = targets.map(function(target) {
      return {
        permitId: target.permit.permit_id,
        companyId: target.permit.company_id,
        status: 'QUEUED'
      };
    });
    var audit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'REQUEST_MLIT_REFRESH',
      target_type: 'Permit',
      target_id: plannedItems.map(function(item) { return item.permitId; }).join(','),
      request_id: requestId,
      reason_note: reason,
      details: JSON.stringify(plannedItems),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    targets.forEach(function(target) {
      var observation = ensureMlitObservationForPermit_(target.permit, target.company);
      var current = findMlitObservationForPermit_(target.permit) || observation;
      updateRecord_(SHEETS.MLITPermits, current._row, {
        refresh_requested_at: requestedAt,
        refresh_requested_by: user.email,
        refresh_priority: 'USER_REQUEST',
        next_retry_at: ''
      });
    });
    var auditRow = findByKey_(SHEETS.AuditLog, 'log_id', audit.log_id);
    if (!auditRow) {
      throw appError_(
        'COMMITTED_AUDIT_PENDING',
        '確認予約は反映されましたが監査ログ確定が必要です',
        false,
        audit.log_id
      );
    }
    updateRecord_(SHEETS.AuditLog, auditRow._row, { status: 'COMMITTED' });
    SpreadsheetApp.flush();
    return {
      items: plannedItems,
      requestedAt: requestedAt,
      auditId: audit.log_id,
      message: '次回のMLIT確認枠で処理します'
    };
  } finally {
    lock.releaseLock();
  }
}

function applyMlitExpiryDiffSecure_(payload, user, requestId) {
  assertOnlyKeys_(
    payload,
    ['permitId', 'permitDataVersion', 'reason'],
    'mlit.applyDiff'
  );
  if (getMlitSyncMode_() !== 'MANUAL_APPLY') {
    throw appError_('MLIT_MODE_BLOCKED', 'MANUAL_APPLY modeでのみ反映できます', false);
  }
  var permitId = normalizeTextInput_(payload.permitId, 100, '許可ID');
  var expectedVersion = Number(payload.permitDataVersion);
  var reason = normalizeTextInput_(payload.reason, 200, '反映理由');
  if (!permitId || !Number.isInteger(expectedVersion) || expectedVersion < 1 || !reason) {
    throw appError_('INVALID_PAYLOAD', '許可ID、version、反映理由が必要です', false);
  }
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', '期限反映が混雑しています', true);
  var preparedAudit = null;
  try {
    var permit = findByKey_(SHEETS.Permits, 'permit_id', permitId);
    if (!permit) throw appError_('PERMIT_NOT_FOUND', '許可情報がありません', false);
    if (getPermitDataVersion_(permit) !== expectedVersion) {
      throw appError_('STALE_VERSION', '許可情報が先に更新されました', true);
    }
    var observation = findMlitObservationForPermit_(permit);
    if (!observation ||
        String(observation.diff_status || '').toUpperCase() !== 'PENDING_REVIEW') {
      throw appError_('DIFF_NOT_PENDING', '反映待ちのMLIT差分がありません', false);
    }
    if (['NOT_FOUND', 'AMBIGUOUS_MATCH', 'IDENTITY_MISMATCH'].indexOf(
      String(observation.diff_type || '').toUpperCase()
    ) >= 0) {
      throw appError_(
        'MLIT_DIFF_NOT_APPLICABLE',
        '照合先を確認できない差分は期限へ反映できません',
        false
      );
    }
    var observedExpiry = normalizeExpiryDateKey_(
      observation.observed_expiry_date || observation.expiry_date
    );
    if (!observedExpiry) {
      throw appError_('INVALID_OBSERVED_EXPIRY', 'MLIT観測期限が不正です', false);
    }
    var nextVersion = expectedVersion + 1;
    var now = getNowString_();
    preparedAudit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'APPLY_MLIT_EXPIRY_DIFF',
      target_type: 'Permit',
      target_id: permitId,
      request_id: requestId,
      reason_note: reason,
      before_json: JSON.stringify({
        expiry_date: permit.expiry_date,
        permit_data_version: expectedVersion
      }),
      after_json: JSON.stringify({
        expiry_date: observedExpiry,
        permit_data_version: nextVersion
      }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    try {
      updateRecord_(SHEETS.Permits, permit._row, {
        expiry_date: observedExpiry,
        permit_data_version: nextVersion,
        mlit_confirmed_date: now,
        mlit_confirm_result: 'APPLIED',
        updated_at: now
      });
      SpreadsheetApp.flush();
    } catch (permitWriteError) {
      var abortAudit = findByKey_(SHEETS.AuditLog, 'log_id', preparedAudit.log_id);
      if (abortAudit) {
        updateRecord_(SHEETS.AuditLog, abortAudit._row, {
          status: 'ABORTED',
          error_code: 'PERMIT_WRITE_FAILED',
          details: String(permitWriteError.message || permitWriteError).substring(0, 500)
        });
      }
      throw permitWriteError;
    }
    var staleCount = markPermitQueueItemsStale_(permitId, nextVersion);
    updateRecord_(SHEETS.MLITPermits, observation._row, {
      diff_status: 'APPLIED',
      applied_at: now,
      applied_by: user.email,
      dismissed_at: '',
      dismissed_by: '',
      dismiss_reason: ''
    });
    var preparedRow = findByKey_(SHEETS.AuditLog, 'log_id', preparedAudit.log_id);
    if (!preparedRow) {
      throw appError_(
        'COMMITTED_AUDIT_PENDING',
        '期限は反映されましたが監査ログ確定が必要です',
        false,
        preparedAudit.log_id
      );
    }
    updateRecord_(SHEETS.AuditLog, preparedRow._row, {
      status: 'COMMITTED',
      details: JSON.stringify({ staleQueueItems: staleCount })
    });
    SpreadsheetApp.flush();
    return {
      permitId: permitId,
      expiryDate: observedExpiry,
      permitDataVersion: nextVersion,
      staleQueueItems: staleCount,
      auditId: preparedAudit.log_id
    };
  } finally {
    lock.releaseLock();
  }
}

function dismissMlitExpiryDiffSecure_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['permitId', 'reason'], 'mlit.dismissDiff');
  var permitId = normalizeTextInput_(payload.permitId, 100, '許可ID');
  var reason = normalizeTextInput_(payload.reason, 200, '却下理由');
  if (!permitId || !reason) {
    throw appError_('INVALID_PAYLOAD', '許可IDと却下理由が必要です', false);
  }
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', '差分確認が混雑しています', true);
  try {
    var permit = findByKey_(SHEETS.Permits, 'permit_id', permitId);
    var observation = findMlitObservationForPermit_(permit);
    if (!observation ||
        String(observation.diff_status || '').toUpperCase() !== 'PENDING_REVIEW') {
      throw appError_('DIFF_NOT_PENDING', '確認待ちのMLIT差分がありません', false);
    }
    if (['NOT_FOUND', 'AMBIGUOUS_MATCH', 'IDENTITY_MISMATCH'].indexOf(
      String(observation.diff_type || '').toUpperCase()
    ) >= 0) {
      throw appError_(
        'MLIT_IDENTITY_REQUIRES_CORRECTION',
        '照合先未確定は却下できません。許可識別情報の修正または管理対象外設定が必要です',
        false
      );
    }
    var now = getNowString_();
    var audit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'DISMISS_MLIT_EXPIRY_DIFF',
      target_type: 'Permit',
      target_id: permitId,
      request_id: requestId,
      reason_note: reason,
      before_json: JSON.stringify({
        diff_status: observation.diff_status,
        diff_type: observation.diff_type || ''
      }),
      after_json: JSON.stringify({ diff_status: 'DISMISSED' }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    updateRecord_(SHEETS.MLITPermits, observation._row, {
      diff_status: 'DISMISSED',
      dismissed_at: now,
      dismissed_by: user.email,
      dismiss_reason: reason
    });
    var auditRow = findByKey_(SHEETS.AuditLog, 'log_id', audit.log_id);
    if (!auditRow) {
      throw appError_(
        'COMMITTED_AUDIT_PENDING',
        '却下は反映されましたが監査ログ確定が必要です',
        false,
        audit.log_id
      );
    }
    updateRecord_(SHEETS.AuditLog, auditRow._row, { status: 'COMMITTED' });
    SpreadsheetApp.flush();
    return { permitId: permitId, auditId: audit.log_id };
  } finally {
    lock.releaseLock();
  }
}

function setMlitSyncModeSecure_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['mode', 'reason'], 'operations.setMlitMode');
  var requested = String(payload.mode || '').trim().toUpperCase();
  var reason = normalizeTextInput_(payload.reason, 200, '変更理由');
  if (MLIT_SYNC_MODES_.indexOf(requested) < 0 || !reason) {
    throw appError_('INVALID_MLIT_MODE', '有効なmodeと変更理由が必要です', false);
  }
  var current = getMlitSyncMode_();
  if (requested === current) return { mode: current, changed: false };
  var currentIndex = MLIT_SYNC_MODES_.indexOf(current);
  var requestedIndex = MLIT_SYNC_MODES_.indexOf(requested);
  if (requestedIndex > currentIndex && requestedIndex !== currentIndex + 1) {
    throw appError_('MLIT_MODE_SEQUENCE', 'MLIT modeは1段階ずつ昇格してください', false);
  }
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', 'MLIT mode変更が混雑しています', true);
  try {
    current = getMlitSyncMode_();
    currentIndex = MLIT_SYNC_MODES_.indexOf(current);
    requestedIndex = MLIT_SYNC_MODES_.indexOf(requested);
    if (requested === current) return { mode: current, changed: false };
    if (requestedIndex > currentIndex && requestedIndex !== currentIndex + 1) {
      throw appError_('MLIT_MODE_SEQUENCE', 'MLIT modeは1段階ずつ昇格してください', false);
    }
    var now = getNowString_();
    var audit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'SET_MLIT_SYNC_MODE',
      target_type: 'MlitSyncPolicy',
      target_id: requested,
      request_id: requestId,
      reason_note: reason,
      before_json: JSON.stringify({ mode: current }),
      after_json: JSON.stringify({ mode: requested }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    setSecureSetting_('MLIT_SYNC_MODE', requested);
    setSecureSetting_('MLIT_SYNC_MODE_CHANGED_AT', now);
    setSecureSetting_('MLIT_SYNC_MODE_CHANGED_BY', user.email);
    var auditRow = findByKey_(SHEETS.AuditLog, 'log_id', audit.log_id);
    if (!auditRow) {
      throw appError_(
        'COMMITTED_AUDIT_PENDING',
        'modeは変更されましたが監査ログ確定が必要です',
        false,
        audit.log_id
      );
    }
    updateRecord_(SHEETS.AuditLog, auditRow._row, { status: 'COMMITTED' });
    SpreadsheetApp.flush();
    return { mode: requested, changed: true, auditId: audit.log_id };
  } finally {
    lock.releaseLock();
  }
}

function updateCompanyMonitoringSecure_(payload, user, requestId) {
  assertOnlyKeys_(
    payload,
    ['companyId', 'dataVersion', 'enabled', 'reason'],
    'companies.updateMonitoring'
  );
  var companyId = normalizeTextInput_(payload.companyId, 50, '会社ID');
  var expectedVersion = Number(payload.dataVersion);
  var reason = normalizeTextInput_(payload.reason, 200, '変更理由');
  if (!companyId || !Number.isInteger(expectedVersion) || expectedVersion < 1 ||
      typeof payload.enabled !== 'boolean' || !reason) {
    throw appError_('INVALID_PAYLOAD', '会社ID、version、対象設定、理由が必要です', false);
  }
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', '対象設定が混雑しています', true);
  try {
    var company = findByKey_(SHEETS.Companies, 'company_id', companyId);
    if (!company) throw appError_('NOT_FOUND', '会社が見つかりません', false);
    if (getCompanyVersion_(company) !== expectedVersion) {
      throw appError_('STALE_VERSION', '会社情報が先に更新されました', true);
    }
    var nextVersion = expectedVersion + 1;
    var now = getNowString_();
    var audit = appendAuditEvent_({
      user_email: user.email,
      actor_role: user.role,
      action: 'UPDATE_PERMIT_MONITORING',
      target_type: 'Company',
      target_id: companyId,
      request_id: requestId,
      reason_note: reason,
      before_json: JSON.stringify({
        enabled: isCompanyPermitMonitoringEnabled_(company),
        data_version: expectedVersion
      }),
      after_json: JSON.stringify({
        enabled: payload.enabled,
        data_version: nextVersion
      }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    updateRecord_(SHEETS.Companies, company._row, {
      permit_monitoring_enabled: payload.enabled,
      data_version: nextVersion,
      updated_at: now,
      updated_by: user.email
    });
    var staleCount = markCompanyQueueItemsStale_(companyId, nextVersion);
    var queued = 0;
    if (payload.enabled) {
      readRecords_(SHEETS.Permits).filter(function(permit) {
        return String(permit.company_id || '') === companyId &&
          isTrackablePermit_(permit);
      }).forEach(function(permit) {
        var linked = ensureMlitObservationForPermit_(permit, Object.assign({}, company, {
          permit_monitoring_enabled: true
        }));
        var current = findMlitObservationForPermit_(permit) || linked;
        updateRecord_(SHEETS.MLITPermits, current._row, {
          refresh_requested_at: now,
          refresh_requested_by: user.email,
          refresh_priority: 'MONITORING_ENABLED',
          next_retry_at: ''
        });
        queued++;
      });
    }
    var auditRow = findByKey_(SHEETS.AuditLog, 'log_id', audit.log_id);
    if (!auditRow) {
      throw appError_(
        'COMMITTED_AUDIT_PENDING',
        '対象設定は変更されましたが監査ログ確定が必要です',
        false,
        audit.log_id
      );
    }
    updateRecord_(SHEETS.AuditLog, auditRow._row, {
      details: JSON.stringify({ queuedPermits: queued, staleQueueItems: staleCount }),
      status: 'COMMITTED'
    });
    SpreadsheetApp.flush();
    return {
      company: serializeCompanyForClient_(findByKey_(SHEETS.Companies, 'company_id', companyId)),
      queuedPermits: queued,
      staleQueueItems: staleCount,
      auditId: audit.log_id
    };
  } finally {
    lock.releaseLock();
  }
}

function getMlitOperationsStatus_() {
  var observations = readRecords_(SHEETS.MLITPermits);
  var now = Date.now();
  var staleCount = 0;
  var failedCount = 0;
  var pendingDiffCount = 0;
  var queuedCount = 0;
  observations.forEach(function(observation) {
    var lastSuccess = parseStoredTimestamp_(observation.last_success_at);
    if (!lastSuccess || now - lastSuccess.getTime() > 7 * 86400000) staleCount++;
    if (Number(observation.consecutive_failure_count || 0) > 0 ||
        Number(observation.consecutive_not_found_count || 0) > 0) failedCount++;
    if (String(observation.diff_status || '').toUpperCase() === 'PENDING_REVIEW') {
      pendingDiffCount++;
    }
    if (String(observation.refresh_requested_at || '').trim()) queuedCount++;
  });
  return {
    mode: getMlitSyncMode_(),
    modeChangedAt: getSecureSetting_('MLIT_SYNC_MODE_CHANGED_AT'),
    modeChangedBy: getSecureSetting_('MLIT_SYNC_MODE_CHANGED_BY'),
    totalObservations: observations.length,
    staleOver7Days: staleCount,
    failures: failedCount,
    pendingDiffs: pendingDiffCount,
    queuedRefreshes: queuedCount
  };
}
