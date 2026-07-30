/**
 * MlitRolling.gs — Companies/Permitsを起点にしたMLIT定期観測
 *
 * MLITPermitsは観測値、Permitsは承認済み正本として分離する。
 * 失敗時はlast_attempted_atだけを進め、last_success_at/last_syncedは変更しない。
 */

var MLIT_ROLLING_DEFAULTS_ = {
  TARGET_CYCLE_DAYS: 7,
  MAX_BATCH_SIZE: 25,
  PRE_NOTIFICATION_LIMIT: 10,
  MAX_STALE_DAYS: 7,
  LEASE_SECONDS: 600
};

var MLIT_ROLLING_PAUSE_KEY_ = 'MLIT_ROLLING_PAUSE';
var MLIT_JOB_LEASE_KEY_ = 'MLIT_JOB_LEASE';

function isMlitRollingPaused_() {
  var value = PropertiesService.getScriptProperties()
    .getProperty(MLIT_ROLLING_PAUSE_KEY_);
  return String(value || '').toLowerCase() === 'true';
}

function pauseMlitRolling_() {
  PropertiesService.getScriptProperties()
    .setProperty(MLIT_ROLLING_PAUSE_KEY_, 'true');
}

function resumeMlitRolling_() {
  PropertiesService.getScriptProperties()
    .deleteProperty(MLIT_ROLLING_PAUSE_KEY_);
}

function formatMlitTimestamp_(date) {
  return Utilities.formatDate(date, 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
}

function addMlitRetryHours_(hours) {
  return formatMlitTimestamp_(new Date(Date.now() + Number(hours || 1) * 3600000));
}

function getActiveMonitoredPermitPairs_() {
  var companies = {};
  readRecords_(SHEETS.Companies).forEach(function(company) {
    if (isCompanyPermitMonitoringEnabled_(company)) {
      companies[String(company.company_id || '')] = company;
    }
  });
  return readRecords_(SHEETS.Permits).filter(function(permit) {
    return !!companies[String(permit.company_id || '')] &&
      isTrackablePermit_(permit);
  }).map(function(permit) {
    return {
      permit: permit,
      company: companies[String(permit.company_id || '')]
    };
  });
}

function seedMlitObservationsFromCanonical_() {
  ensureHeaders_(getSheet_(SHEETS.MLITPermits), MLIT_PERMITS_HEADERS_);
  var result = { scanned: 0, linked: 0, created: 0, skipped: 0 };
  getActiveMonitoredPermitPairs_().forEach(function(pair) {
    result.scanned++;
    try {
      var before = findMlitObservationForPermit_(pair.permit);
      var observation = ensureMlitObservationForPermit_(pair.permit, pair.company);
      if (!before && observation) result.created++;
      else if (before && !String(before.permit_id || '').trim()) result.linked++;
    } catch (error) {
      result.skipped++;
      logError_(
        'MLIT観測seedスキップ permit_id=' + String(pair.permit.permit_id || ''),
        error
      );
    }
  });
  return result;
}

function isMlitRetryDue_(observation, nowMs) {
  var retryAt = parseStoredTimestamp_(observation.next_retry_at);
  return !retryAt || retryAt.getTime() <= nowMs;
}

function getMlitCandidatePriority_(observation) {
  if (String(observation.refresh_requested_at || '').trim()) return 0;
  if (!String(observation.last_success_at || '').trim()) return 1;
  if (Number(observation.consecutive_failure_count || 0) > 0 ||
      Number(observation.consecutive_not_found_count || 0) > 0) return 2;
  return 3;
}

function pickMlitRollingCandidates_(limit, maxStaleDays) {
  seedMlitObservationsFromCanonical_();
  var max = Number(limit);
  if (!Number.isInteger(max) || max < 1) max = 1;
  max = Math.min(max, MLIT_ROLLING_DEFAULTS_.MAX_BATCH_SIZE);
  var staleDays = Number(maxStaleDays);
  if (!Number.isFinite(staleDays) || staleDays < 0) {
    staleDays = MLIT_ROLLING_DEFAULTS_.MAX_STALE_DAYS;
  }
  var nowMs = Date.now();
  var staleCutoffMs = nowMs - staleDays * 86400000;
  var activePermitIds = {};
  getActiveMonitoredPermitPairs_().forEach(function(pair) {
    activePermitIds[String(pair.permit.permit_id || '')] = true;
  });
  var manualStatuses = ['MANUAL_ENTRY', 'DUPLICATE_DELETE', 'PERSON_NAME_DELETE'];
  var candidates = readRecords_(SHEETS.MLITPermits).filter(function(observation) {
    var permitId = String(observation.permit_id || '');
    if (!permitId || !activePermitIds[permitId]) return false;
    var fetchStatus = String(observation.fetch_status || '').trim().toUpperCase();
    if (manualStatuses.indexOf(fetchStatus) >= 0 ||
        fetchStatus.indexOf('PERMIT_CORRECTED_') === 0) return false;
    if (!isMlitRetryDue_(observation, nowMs)) return false;
    if (String(observation.refresh_requested_at || '').trim()) return true;
    var lastSuccess = parseStoredTimestamp_(observation.last_success_at);
    return !lastSuccess || lastSuccess.getTime() < staleCutoffMs;
  });
  candidates.sort(function(a, b) {
    var priorityDiff = getMlitCandidatePriority_(a) - getMlitCandidatePriority_(b);
    if (priorityDiff !== 0) return priorityDiff;
    var aSuccess = parseStoredTimestamp_(a.last_success_at);
    var bSuccess = parseStoredTimestamp_(b.last_success_at);
    return (aSuccess ? aSuccess.getTime() : 0) - (bSuccess ? bSuccess.getTime() : 0);
  });
  return candidates.slice(0, max);
}

function resolveMlitPermitRow_(observation) {
  var permitId = String(observation && observation.permit_id || '').trim();
  var companyId = String(observation && observation.company_id || '').trim();
  var authority = String(observation && observation.authority || '').trim();
  var permitNumber = normalizePermitNumberForMlit_(
    observation && observation.permit_number
  );
  var rows = readRecords_(SHEETS.MLITPermits);
  for (var i = 0; i < rows.length; i++) {
    if (permitId && String(rows[i].permit_id || '').trim() === permitId) {
      return rows[i];
    }
  }
  for (var j = 0; j < rows.length; j++) {
    if (String(rows[j].company_id || '').trim() === companyId &&
        String(rows[j].authority || '').trim() === authority &&
        normalizePermitNumberForMlit_(rows[j].permit_number) === permitNumber) {
      return rows[j];
    }
  }
  return null;
}

function recordMlitAttempt_(observation, nowString) {
  var current = resolveMlitPermitRow_(observation);
  if (!current) return null;
  updateRecord_(SHEETS.MLITPermits, current._row, {
    last_attempted_at: nowString,
    refresh_requested_at: '',
    refresh_requested_by: '',
    refresh_priority: ''
  });
  return resolveMlitPermitRow_(observation);
}

function recordMlitTransientFailure_(observation, errorCode, message, nowString) {
  var current = resolveMlitPermitRow_(observation);
  if (!current) return;
  var failureCount = Number(current.consecutive_failure_count || 0) + 1;
  var retryHours = Math.min(Math.pow(2, failureCount - 1), 24);
  updateRecord_(SHEETS.MLITPermits, current._row, {
    last_attempted_at: nowString,
    next_retry_at: addMlitRetryHours_(retryHours),
    consecutive_failure_count: failureCount,
    fetch_status: 'TRANSIENT_ERROR',
    last_error_code: String(errorCode || 'MLIT_ERROR'),
    last_error_message: String(message || '').substring(0, 500),
    refresh_requested_at: '',
    refresh_requested_by: '',
    refresh_priority: ''
  });
}

function recordMlitNotFound_(observation, nowString) {
  var current = resolveMlitPermitRow_(observation);
  if (!current) return { count: 0, pendingReview: false };
  var count = Number(current.consecutive_not_found_count || 0) + 1;
  var pendingReview = count >= 3;
  var retryHours = count === 1 ? 1 : count === 2 ? 6 : 24;
  var updates = {
    last_attempted_at: nowString,
    next_retry_at: addMlitRetryHours_(retryHours),
    consecutive_failure_count: 0,
    consecutive_not_found_count: count,
    fetch_status: pendingReview ? 'NOT_FOUND_AT_REFRESH' : 'NOT_FOUND_RETRY',
    last_error_code: 'NOT_FOUND',
    last_error_message: 'MLIT検索結果が0件です（連続' + count + '回）',
    refresh_requested_at: '',
    refresh_requested_by: '',
    refresh_priority: ''
  };
  if (pendingReview) {
    updates.diff_status = 'PENDING_REVIEW';
    updates.diff_detected_at = current.diff_detected_at || nowString;
    updates.diff_type = 'NOT_FOUND';
    updates.diff_risk_flags = 'MLIT_NOT_FOUND_3_TIMES';
  }
  updateRecord_(SHEETS.MLITPermits, current._row, updates);
  return { count: count, pendingReview: pendingReview };
}

function recordMlitAmbiguousMatch_(observation, candidateCount, nowString) {
  var current = resolveMlitPermitRow_(observation);
  if (!current) return;
  updateRecord_(SHEETS.MLITPermits, current._row, {
    last_attempted_at: nowString,
    next_retry_at: addMlitRetryHours_(24),
    consecutive_failure_count: 0,
    fetch_status: 'AMBIGUOUS_MATCH',
    last_error_code: 'AMBIGUOUS_MATCH',
    last_error_message: 'MLIT検索候補が' + candidateCount + '件あります',
    refresh_requested_at: '',
    refresh_requested_by: '',
    refresh_priority: '',
    diff_status: 'PENDING_REVIEW',
    diff_detected_at: current.diff_detected_at || nowString,
    diff_type: 'AMBIGUOUS_MATCH',
    diff_risk_flags: 'MULTIPLE_MLIT_CANDIDATES'
  });
}

function recordMlitSuccess_(observation, detail, nowString) {
  var current = resolveMlitPermitRow_(observation);
  if (!current) throw appError_('MLIT_ROW_MISSING', 'MLIT観測行が見つかりません', true);
  var permit = findByKey_(SHEETS.Permits, 'permit_id', current.permit_id);
  if (!permit) throw appError_('PERMIT_NOT_FOUND', '許可正本が見つかりません', false);
  var observedExpiry = normalizeExpiryDateKey_(detail.expiryTo);
  if (!observedExpiry) {
    throw appError_('MLIT_EXPIRY_INVALID', 'MLITの有効期限を解釈できません', true);
  }
  var approvedExpiry = normalizeExpiryDateKey_(permit.expiry_date);
  var difference = classifyMlitExpiryDifference_(approvedExpiry, observedExpiry);
  var company = findByKey_(SHEETS.Companies, 'company_id', permit.company_id);
  var approvedCompanyName = company
    ? String(company.company_name_normalized || company.company_name_raw || '')
    : String(permit.company_name_raw || '');
  var observedCompanyName = String(detail.apiName || '').trim();
  var identityMismatch = !!observedCompanyName && !!approvedCompanyName &&
    normalizeMlitCompanyIdentity_(observedCompanyName) !==
      normalizeMlitCompanyIdentity_(approvedCompanyName);
  var previousObserved = normalizeExpiryDateKey_(
    current.observed_expiry_date || current.expiry_date
  );
  var previousObservedCompanyName = String(current.observed_company_name || '').trim();
  var previousDiffStatus = String(current.diff_status || 'NONE').toUpperCase();
  var diffStatus = 'PENDING_REVIEW';
  var diffDetectedAt = current.diff_detected_at || nowString;
  if (difference.type === 'NONE' && !identityMismatch) {
    diffStatus = 'NONE';
    diffDetectedAt = '';
  } else if (previousDiffStatus === 'DISMISSED' &&
             previousObserved === observedExpiry &&
             previousObservedCompanyName === observedCompanyName) {
    diffStatus = 'DISMISSED';
  } else if (previousObserved !== observedExpiry ||
             previousObservedCompanyName !== observedCompanyName ||
             previousDiffStatus !== 'PENDING_REVIEW') {
    diffDetectedAt = nowString;
  }
  var riskFlags = difference.riskFlags.slice();
  var diffType = difference.type;
  if (identityMismatch) {
    diffType = 'IDENTITY_MISMATCH';
    riskFlags.push('COMPANY_NAME_MISMATCH');
  }

  var hasIppan = detail.tradesIppan && detail.tradesIppan.length > 0;
  var hasTokutei = detail.tradesTokutei && detail.tradesTokutei.length > 0;
  var category = hasIppan && hasTokutei ? '般特' : hasIppan ? '般' : hasTokutei ? '特' : '';
  var allTrades = {};
  (detail.tradesIppan || []).forEach(function(trade) { if (trade) allTrades[trade] = true; });
  (detail.tradesTokutei || []).forEach(function(trade) { if (trade) allTrades[trade] = true; });
  updateRecord_(SHEETS.MLITPermits, current._row, {
    expiry_date: observedExpiry,
    observed_expiry_date: observedExpiry,
    expiry_wareki: detail.expiryWareki || '',
    days_remaining: resolveDaysRemaining_(observedExpiry, null),
    trades_ippan: (detail.tradesIppan || []).join('|'),
    trades_tokutei: (detail.tradesTokutei || []).join('|'),
    trades_count: Object.keys(allTrades).length,
    category: category,
    fetch_status: 'OK',
    last_synced: nowString,
    last_attempted_at: nowString,
    last_success_at: nowString,
    next_retry_at: '',
    consecutive_failure_count: 0,
    consecutive_not_found_count: 0,
    last_error_code: '',
    last_error_message: '',
    refresh_requested_at: '',
    refresh_requested_by: '',
    refresh_priority: '',
    diff_status: diffStatus,
    diff_detected_at: diffDetectedAt,
    observed_company_name: observedCompanyName,
    diff_type: diffStatus === 'NONE' ? 'NONE' : diffType,
    diff_risk_flags: diffStatus === 'NONE' ? '' : riskFlags.join('|')
  });
  return {
    diffStatus: diffStatus,
    approvedExpiry: approvedExpiry,
    observedExpiry: observedExpiry,
    diffType: diffType,
    riskFlags: riskFlags
  };
}

function refreshOneMlitPermit_(observation) {
  var authority = String(observation.authority || '').trim();
  var permitNumber = String(observation.permit_number || '').trim();
  var nowString = getNowString_();
  if (!authority || !permitNumber) {
    recordMlitTransientFailure_(
      observation,
      'MLIT_IDENTIFIER_MISSING',
      'authority/permit_number欠損',
      nowString
    );
    return { result: '確認不可', message: 'authority/permit_number欠損' };
  }
  recordMlitAttempt_(observation, nowString);
  var licenseNoKbn = getLicenseNoKbn_(authority);
  var prefCode = getPrefCode_(authority);
  var candidates;
  try {
    candidates = withMlitRateLimit_(function() {
      return searchMlitPermit_(licenseNoKbn, permitNumber, prefCode);
    }, { maxWaitMs: 30000 });
  } catch (error) {
    recordMlitTransientFailure_(
      observation,
      String(error.code || 'MLIT_SEARCH_ERROR'),
      error.message || String(error),
      nowString
    );
    return { result: '確認不可', message: 'search error: ' + error.message };
  }
  if (!candidates || candidates.length === 0) {
    var notFound = recordMlitNotFound_(observation, nowString);
    return {
      result: notFound.pendingReview ? '差分' : '確認不可',
      message: 'NOT_FOUND count=' + notFound.count
    };
  }
  if (candidates.length > 1) {
    recordMlitAmbiguousMatch_(observation, candidates.length, nowString);
    return {
      result: '差分',
      message: 'AMBIGUOUS_MATCH count=' + candidates.length
    };
  }

  var detail;
  try {
    detail = withMlitRateLimit_(function() {
      return fetchMlitDetail_(candidates[0]);
    }, { maxWaitMs: 30000 });
  } catch (error) {
    recordMlitTransientFailure_(
      observation,
      String(error.code || 'MLIT_DETAIL_ERROR'),
      error.message || String(error),
      nowString
    );
    return { result: '確認不可', message: 'detail error: ' + error.message };
  }
  if (!detail || !detail.found) {
    recordMlitTransientFailure_(
      observation,
      'MLIT_DETAIL_INVALID',
      detail && detail.error ? detail.error : 'detail not found',
      nowString
    );
    return { result: '確認不可', message: 'detail invalid' };
  }
  try {
    var success = recordMlitSuccess_(observation, detail, nowString);
    return {
      result: success.diffStatus === 'PENDING_REVIEW' ? '差分' : '一致',
      message: success.diffStatus +
        ' type=' + success.diffType +
        ' risks=' + success.riskFlags.join('|') +
        ' approved=' + success.approvedExpiry +
        ' observed=' + success.observedExpiry
    };
  } catch (error) {
    recordMlitTransientFailure_(
      observation,
      String(error.code || 'MLIT_RESULT_ERROR'),
      error.message || String(error),
      nowString
    );
    return { result: '確認不可', message: error.message || String(error) };
  }
}

function tryAcquireMlitJobLease_(jobName) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return null;
  try {
    var properties = PropertiesService.getScriptProperties();
    var raw = properties.getProperty(MLIT_JOB_LEASE_KEY_);
    var current = null;
    try {
      current = raw ? JSON.parse(raw) : null;
    } catch (ignoredInvalidLease) {
      current = null;
    }
    if (current && Number(current.expiresAt || 0) > Date.now()) return null;
    var token = generateUuid_();
    properties.setProperty(MLIT_JOB_LEASE_KEY_, JSON.stringify({
      token: token,
      jobName: jobName,
      expiresAt: Date.now() + MLIT_ROLLING_DEFAULTS_.LEASE_SECONDS * 1000
    }));
    return token;
  } finally {
    lock.releaseLock();
  }
}

function releaseMlitJobLease_(token) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return false;
  try {
    var properties = PropertiesService.getScriptProperties();
    var raw = properties.getProperty(MLIT_JOB_LEASE_KEY_);
    var current = null;
    try {
      current = raw ? JSON.parse(raw) : null;
    } catch (ignoredInvalidLease) {
      current = null;
    }
    if (current && current.token === token) {
      properties.deleteProperty(MLIT_JOB_LEASE_KEY_);
      return true;
    }
    return false;
  } finally {
    lock.releaseLock();
  }
}

function runMlitCandidateBatch_(jobName, candidates) {
  if (getMlitSyncMode_() === 'OFF' || isMlitRollingPaused_()) {
    return { success: false, skipped: true, reason: 'MLIT_SYNC_OFF' };
  }
  var leaseToken = tryAcquireMlitJobLease_(jobName);
  if (!leaseToken) {
    return { success: false, skipped: true, reason: 'MLIT_JOB_ALREADY_RUNNING' };
  }
  var counts = { '一致': 0, '差分': 0, '確認不可': 0 };
  var startedAt = getNowString_();
  try {
    candidates.forEach(function(observation) {
      if (isMlitRollingPaused_()) return;
      var result = refreshOneMlitPermit_(observation);
      counts[result.result] = (counts[result.result] || 0) + 1;
      Logger.log(
        jobName + ' permit_id=' + String(observation.permit_id || '') +
        ' result=' + result.result + ' message=' + result.message
      );
    });
    appendAuditEvent_({
      user_email: 'SYSTEM_MLIT',
      action: 'MLIT_SYNC_BATCH',
      target_type: 'MlitSync',
      target_id: jobName,
      details: JSON.stringify({
        startedAt: startedAt,
        picked: candidates.length,
        counts: counts
      }),
      status: 'COMMITTED'
    });
    return {
      success: true,
      picked: candidates.length,
      counts: counts
    };
  } catch (error) {
    appendAuditEvent_({
      user_email: 'SYSTEM_MLIT',
      action: 'MLIT_SYNC_BATCH',
      target_type: 'MlitSync',
      target_id: jobName,
      details: String(error.message || error).substring(0, 500),
      status: 'ABORTED',
      error_code: String(error.code || 'MLIT_BATCH_FAILED')
    });
    throw error;
  } finally {
    releaseMlitJobLease_(leaseToken);
  }
}

function getDynamicMlitDailyLimit_() {
  var configured = Number(
    PropertiesService.getScriptProperties().getProperty('MLIT_ROLLING_DAILY_LIMIT')
  );
  if (Number.isInteger(configured) && configured > 0) {
    return Math.min(configured, MLIT_ROLLING_DEFAULTS_.MAX_BATCH_SIZE);
  }
  var activePermitCount = getActiveMonitoredPermitPairs_().length;
  if (activePermitCount === 0) return 1;
  return Math.min(
    Math.ceil(activePermitCount / MLIT_ROLLING_DEFAULTS_.TARGET_CYCLE_DAYS) + 2,
    MLIT_ROLLING_DEFAULTS_.MAX_BATCH_SIZE
  );
}

function runDailyMlitRolling_() {
  if (getMlitSyncMode_() === 'OFF' || isMlitRollingPaused_()) {
    return { success: false, skipped: true, reason: 'MLIT_SYNC_OFF' };
  }
  var limit = getDynamicMlitDailyLimit_();
  var candidates = pickMlitRollingCandidates_(
    limit,
    MLIT_ROLLING_DEFAULTS_.MAX_STALE_DAYS
  );
  return runMlitCandidateBatch_('NIGHTLY_SWEEP', candidates);
}

function pickPreNotificationMlitCandidates_() {
  seedMlitObservationsFromCanonical_();
  var stageDays = parseNotifyStages_(getConfig_('NOTIFY_STAGES_DAYS'));
  var largestStage = Math.max.apply(null, stageDays);
  var nowMs = Date.now();
  var candidates = [];
  getActiveMonitoredPermitPairs_().forEach(function(pair) {
    var days = daysUntil_(pair.permit.expiry_date);
    if (isNaN(days) || days > largestStage) return;
    var observation = findMlitObservationForPermit_(pair.permit);
    if (!observation ||
        String(observation.diff_status || '').toUpperCase() === 'PENDING_REVIEW' ||
        !isMlitRetryDue_(observation, nowMs)) return;
    var lastSuccess = parseStoredTimestamp_(observation.last_success_at);
    if (!lastSuccess || nowMs - lastSuccess.getTime() > 24 * 3600000) {
      candidates.push(observation);
    }
  });
  candidates.sort(function(a, b) {
    var aSuccess = parseStoredTimestamp_(a.last_success_at);
    var bSuccess = parseStoredTimestamp_(b.last_success_at);
    return (aSuccess ? aSuccess.getTime() : 0) - (bSuccess ? bSuccess.getTime() : 0);
  });
  return candidates.slice(0, MLIT_ROLLING_DEFAULTS_.PRE_NOTIFICATION_LIMIT);
}

function runPreNotificationMlitRefresh_() {
  if (getMlitSyncMode_() === 'OFF' || isMlitRollingPaused_()) {
    return { success: false, skipped: true, reason: 'MLIT_SYNC_OFF' };
  }
  return runMlitCandidateBatch_(
    'PRE_NOTIFICATION',
    pickPreNotificationMlitCandidates_()
  );
}

function debugPickMlitRollingCandidates_() {
  var candidates = pickMlitRollingCandidates_(getDynamicMlitDailyLimit_(), 7);
  Logger.log(JSON.stringify(candidates.map(function(candidate) {
    return {
      permit_id: candidate.permit_id,
      company_id: candidate.company_id,
      last_success_at: candidate.last_success_at,
      refresh_requested_at: candidate.refresh_requested_at
    };
  })));
  return candidates;
}

function debugRefreshOneByCompanyId_(companyId) {
  var permit = readRecords_(SHEETS.Permits).filter(function(row) {
    return String(row.company_id || '') === String(companyId || '');
  })[0];
  if (!permit) throw new Error('company_idに紐づく許可情報がありません');
  var company = findByKey_(SHEETS.Companies, 'company_id', companyId);
  var observation = ensureMlitObservationForPermit_(permit, company);
  return refreshOneMlitPermit_(observation);
}
