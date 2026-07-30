/**
 * NotificationQueue.gs — 通知候補生成、手動確認、段階自動化policy
 */

var NOTIFICATION_MODES_ = [
  'OFF',
  'INTERNAL_TEST',
  'MANUAL_PILOT',
  'MANUAL_ALL',
  'AUTO_LOW_PILOT',
  'AUTO_LOW_ALL',
  'AUTO_STANDARD_ALL',
  'AUTO_ALL'
];

var QUEUE_OPEN_STATUSES_ = [
  'DRAFT', 'READY', 'APPROVED', 'SENDING', 'PENDING_RECONCILIATION'
];

function getNotificationMode_() {
  var mode = String(getSecureSetting_('NOTIFICATION_MODE') || 'OFF')
    .trim()
    .toUpperCase();
  return NOTIFICATION_MODES_.indexOf(mode) >= 0 ? mode : 'OFF';
}

function getPilotCompanySet_() {
  var set = {};
  String(getSecureSetting_('PILOT_COMPANY_IDS') || '')
    .split(',')
    .map(function(value) { return value.trim(); })
    .filter(Boolean)
    .forEach(function(companyId) { set[companyId] = true; });
  return set;
}

function getModePolicy_(mode) {
  var normalized = NOTIFICATION_MODES_.indexOf(mode) >= 0 ? mode : 'OFF';
  return {
    mode: normalized,
    manualAllowed: [
      'INTERNAL_TEST', 'MANUAL_PILOT', 'MANUAL_ALL',
      'AUTO_LOW_PILOT', 'AUTO_LOW_ALL', 'AUTO_STANDARD_ALL', 'AUTO_ALL'
    ].indexOf(normalized) >= 0,
    autoAllowed: [
      'AUTO_LOW_PILOT', 'AUTO_LOW_ALL', 'AUTO_STANDARD_ALL', 'AUTO_ALL'
    ].indexOf(normalized) >= 0,
    pilotOnly: normalized === 'MANUAL_PILOT' || normalized === 'AUTO_LOW_PILOT',
    internalOverride: normalized === 'INTERNAL_TEST',
    autoStages: normalized === 'AUTO_ALL'
      ? ['90', '60', '30', '0', 'EXPIRED']
      : normalized === 'AUTO_STANDARD_ALL'
        ? ['90', '60', '30', '0']
        : ['90', '60']
  };
}

function normalizeQueueAddendum_(value) {
  var text = String(value || '').trim();
  if (text.length > 1000) {
    throw appError_('INVALID_ADDENDUM', '追記は1000文字以内で入力してください', false);
  }
  if (/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(text)) {
    throw appError_('INVALID_ADDENDUM', '追記に使用できない制御文字があります', false);
  }
  return text;
}

function serializeQueueItem_(queue) {
  var company = findByKey_(SHEETS.Companies, 'company_id', queue.company_id);
  var permit = findByKey_(SHEETS.Permits, 'permit_id', queue.permit_id);
  return {
    queue_id: String(queue.queue_id || ''),
    idempotency_key: String(queue.idempotency_key || ''),
    company_id: String(queue.company_id || ''),
    company_name: company
      ? String(company.company_name_normalized || company.company_name_raw || '')
      : '',
    permit_id: String(queue.permit_id || ''),
    permit_number: permit ? String(permit.permit_number_full || '') : '',
    expiry_date: permit ? permit.expiry_date || '' : '',
    stage: String(queue.stage || ''),
    source_data_version: Number(queue.source_data_version || 0),
    source_company_version: Number(
      queue.source_company_version || queue.source_data_version || 0
    ),
    source_permit_version: Number(queue.source_permit_version || 0),
    source_expiry_date: queue.source_expiry_date || '',
    to_email: normalizeEmailAddress_(queue.to_email),
    cc_email: String(queue.cc_email || ''),
    bcc_email: String(queue.bcc_email || ''),
    subject: String(queue.subject || ''),
    body_template: String(queue.body_template || ''),
    addendum: String(queue.addendum || ''),
    status: String(queue.status || ''),
    send_origin: String(queue.send_origin || ''),
    created_at: queue.created_at || '',
    created_by: String(queue.created_by || ''),
    approved_at: queue.approved_at || '',
    approved_by: normalizeEmailAddress_(queue.approved_by),
    sent_at: queue.sent_at || '',
    notification_id: String(queue.notification_id || ''),
    error_code: String(queue.error_code || ''),
    error_message: String(queue.error_message || '')
  };
}

function getQueueById_(queueId) {
  return findByKey_(SHEETS.NotificationQueue, 'queue_id', String(queueId || ''));
}

function findLatestQueueByIdempotency_(key) {
  var rows = readRecords_(SHEETS.NotificationQueue);
  for (var i = rows.length - 1; i >= 0; i--) {
    if (String(rows[i].idempotency_key || '') === String(key || '')) return rows[i];
  }
  return null;
}

function hasReservedOrSentNotificationKey_(idempotencyKey) {
  var rows = readRecords_(SHEETS.Notifications);
  return rows.some(function(row) {
    return String(row.idempotency_key || '') === String(idempotencyKey || '') &&
      ['PENDING', 'SENT'].indexOf(String(row.result || '')) >= 0;
  });
}

function buildNotificationIdempotencyKey_(permitId, expiryDate, stage) {
  var expiryKey = normalizeExpiryDateKey_(expiryDate);
  if (!expiryKey) {
    throw appError_('INVALID_EXPIRY_DATE', '通知候補の有効期限が不正です', false);
  }
  return 'PERMIT:' + permitId + ':EXPIRY:' + expiryKey + ':STAGE:' + stage;
}

function determineCandidateStage_(days, stageDays, permitId, expiryDate) {
  var stage = days < 0 ? 'EXPIRED' : null;
  if (stage === null) {
    for (var i = stageDays.length - 1; i >= 0; i--) {
      if (days <= stageDays[i]) {
        stage = String(stageDays[i]);
        break;
      }
    }
  }
  if (stage === null) return null;

  var key = buildNotificationIdempotencyKey_(permitId, expiryDate, stage);
  if (hasReservedOrSentNotificationKey_(key)) return null;
  var existing = findLatestQueueByIdempotency_(key);
  if (!existing ||
      ['STALE', 'CANCELLED', 'FAILED', 'BLOCKED'].indexOf(
        String(existing.status || '')
      ) >= 0) {
    return stage;
  }
  return null;
}

function buildQueueRecord_(permit, company, stage, actorEmail) {
  var built = Mailer.buildExpiryNotification(permit, company, stage);
  var readiness = getCompanyReadiness_(company);
  var mlitState = getMlitObservationState_(permit);
  var sendReady = readiness.sendReady && mlitState.sendFresh;
  var blockedCode = readiness.sendReady ? mlitState.code : readiness.code;
  var blockedLabel = readiness.sendReady ? mlitState.label : readiness.label;
  var now = getNowString_();
  return {
    queue_id: generateUuid_(),
    idempotency_key: buildNotificationIdempotencyKey_(
      permit.permit_id, permit.expiry_date, stage
    ),
    company_id: company.company_id,
    permit_id: permit.permit_id,
    stage: String(stage),
    source_data_version: getCompanyVersion_(company),
    source_company_version: getCompanyVersion_(company),
    source_permit_version: getPermitDataVersion_(permit),
    source_expiry_date: normalizeExpiryDateKey_(permit.expiry_date),
    to_email: built.to_email,
    cc_email: built.cc_email,
    bcc_email: built.bcc_email,
    subject: built.subject,
    body_template: built.body,
    addendum: '',
    status: sendReady ? 'READY' : 'BLOCKED',
    send_origin: '',
    created_at: now,
    created_by: actorEmail || 'SYSTEM',
    approved_at: '',
    approved_by: '',
    sending_at: '',
    sent_at: '',
    cancelled_at: '',
    updated_at: now,
    notification_id: '',
    error_code: sendReady ? '' : blockedCode,
    error_message: sendReady ? '' : blockedLabel
  };
}

function createOrRefreshQueueCandidate_(permit, company, stage, actorEmail) {
  var record = buildQueueRecord_(permit, company, stage, actorEmail);
  var previous = findLatestQueueByIdempotency_(record.idempotency_key);
  if (previous && String(previous.status || '') === 'BLOCKED') {
    updateRecord_(SHEETS.NotificationQueue, previous._row, Object.assign({}, record, {
      queue_id: previous.queue_id,
      created_at: previous.created_at || record.created_at
    }));
    return { created: false, refreshed: true, record: getQueueById_(previous.queue_id) };
  }
  appendRecord_(SHEETS.NotificationQueue, record);
  return { created: true, refreshed: false, record: record };
}

function generateNotificationCandidates_(actorEmail) {
  ensureHeaders_(getSheet_(SHEETS.NotificationQueue), NOTIFICATION_QUEUE_HEADERS);
  ensureHeaders_(getSheet_(SHEETS.Notifications), SECURE_NOTIFICATIONS_HEADERS_);
  var stageDays = parseNotifyStages_(getConfig_('NOTIFY_STAGES_DAYS'));
  var permits = PermitsModel.getAllActive();
  var result = { scanned: permits.length, created: 0, refreshed: 0, blocked: 0, errors: 0 };

  permits.forEach(function(permit) {
    try {
      var company = findByKey_(SHEETS.Companies, 'company_id', permit.company_id);
      if (!company || !isCompanyPermitMonitoringEnabled_(company)) return;
      var days = daysUntil_(permit.expiry_date);
      if (isNaN(days)) return;
      var stage = determineCandidateStage_(
        days, stageDays, permit.permit_id, permit.expiry_date
      );
      if (stage === null) return;
      var outcome = createOrRefreshQueueCandidate_(
        permit, company, stage, actorEmail || 'SYSTEM'
      );
      if (outcome.created) result.created++;
      if (outcome.refreshed) result.refreshed++;
      if (String(outcome.record.status || '') === 'BLOCKED') result.blocked++;
    } catch (error) {
      result.errors++;
      logError_('通知候補生成エラー permit_id=' + permit.permit_id, error);
    }
  });
  return result;
}

function listNotificationCandidates_(payload) {
  assertOnlyKeys_(payload, ['status', 'limit'], 'notifications.listCandidates');
  var requestedStatus = String(payload.status || '').trim().toUpperCase();
  var limit = payload.limit === undefined ? 100 : Number(payload.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
    throw appError_('INVALID_LIMIT', 'limitは1〜200で指定してください', false);
  }
  var rows = readRecords_(SHEETS.NotificationQueue);
  if (requestedStatus) {
    rows = rows.filter(function(row) {
      return String(row.status || '').toUpperCase() === requestedStatus;
    });
  }
  rows.sort(function(a, b) {
    return String(a.created_at || '') < String(b.created_at || '') ? 1 : -1;
  });
  return {
    mode: getNotificationMode_(),
    items: rows.slice(0, limit).map(serializeQueueItem_)
  };
}

function validateQueueIds_(payload, label) {
  assertOnlyKeys_(payload, ['queueIds'], label);
  if (!Array.isArray(payload.queueIds) ||
      payload.queueIds.length < 1 ||
      payload.queueIds.length > 10) {
    throw appError_('INVALID_SELECTION', '通知候補は1〜10件選択してください', false);
  }
  var seen = {};
  return payload.queueIds.map(function(value) {
    var id = normalizeTextInput_(value, 100, 'queueId');
    if (!id || seen[id]) throw appError_('INVALID_SELECTION', 'queueIdが不正です', false);
    seen[id] = true;
    return id;
  });
}

function previewNotificationCandidates_(payload) {
  var ids = validateQueueIds_(payload, 'notifications.preview');
  var items = ids.map(function(id) {
    var queue = getQueueById_(id);
    if (!queue) throw appError_('NOT_FOUND', '通知候補が見つかりません', false);
    return serializeQueueItem_(queue);
  });
  var recipients = {};
  items.forEach(function(item) {
    [item.to_email].concat(
      normalizeEmailRecipients_(item.cc_email),
      normalizeEmailRecipients_(item.bcc_email)
    ).forEach(function(email) {
      if (email) recipients[normalizeEmailAddress_(email)] = true;
    });
  });
  return {
    mode: getNotificationMode_(),
    items: items,
    companyCount: Object.keys(items.reduce(function(acc, item) {
      acc[item.company_id] = true;
      return acc;
    }, {})).length,
    recipientCount: Object.keys(recipients).length
  };
}

function validateQueuedSendPolicy_(queueContext) {
  var context = queueContext || {};
  var queue = getQueueById_(context.queueId);
  if (!queue) throw appError_('QUEUE_NOT_FOUND', '通知候補が見つかりません', false);
  var origin = String(context.origin || '').toUpperCase();
  var expectedStatuses = origin === 'AUTO' ? ['READY'] : ['APPROVED'];
  if (expectedStatuses.indexOf(String(queue.status || '')) < 0) {
    throw appError_('QUEUE_STATE_CHANGED', '通知候補の状態が変更されています', true);
  }

  var mode = getNotificationMode_();
  var policy = getModePolicy_(mode);
  if (origin === 'MANUAL' && !policy.manualAllowed) {
    throw appError_('MODE_BLOCKED', '現在のmodeでは手動外部送信できません', false);
  }
  if (origin === 'AUTO' && !policy.autoAllowed) {
    throw appError_('MODE_BLOCKED', '現在のmodeでは自動送信できません', false);
  }

  if (origin === 'MANUAL') {
    var actor = getUserAccessByEmail_(context.actorEmail);
    if (!actor || !actor.active || !actor.canSendExternal ||
        (ROLE_CAPABILITIES_[actor.role] || []).indexOf('notifications.send') < 0) {
      throw appError_('FORBIDDEN_SEND', '外部送信担当者として登録されていません', false);
    }
  }

  var company = findByKey_(SHEETS.Companies, 'company_id', queue.company_id);
  if (!company) throw appError_('COMPANY_NOT_FOUND', '会社が見つかりません', false);
  var queuedCompanyVersion = Number(
    queue.source_company_version || queue.source_data_version || 0
  );
  if (getCompanyVersion_(company) !== queuedCompanyVersion) {
    throw appError_('STALE_QUEUE', '会社マスタ更新後のため候補を再生成してください', false);
  }
  var permit = findByKey_(SHEETS.Permits, 'permit_id', queue.permit_id);
  if (!permit) throw appError_('PERMIT_NOT_FOUND', '許可情報が見つかりません', false);
  if (getPermitDataVersion_(permit) !== Number(queue.source_permit_version || 0) ||
      normalizeExpiryDateKey_(permit.expiry_date) !==
        normalizeExpiryDateKey_(queue.source_expiry_date)) {
    throw appError_('STALE_QUEUE', '許可情報更新後のため候補を再生成してください', false);
  }
  assertPermitFreshForSend_(permit, company);
  var readiness = getCompanyReadiness_(company);
  if (!readiness.sendReady) {
    throw appError_('CONTACT_NOT_READY', readiness.label, false);
  }
  if (policy.pilotOnly && !getPilotCompanySet_()[String(queue.company_id || '')]) {
    throw appError_('PILOT_ONLY', 'pilot対象外の会社です', false);
  }
  if (origin === 'AUTO' && policy.autoStages.indexOf(String(queue.stage || '')) < 0) {
    throw appError_('STAGE_MANUAL_ONLY', 'この通知段階は手動確認が必要です', false);
  }

  var resolved = {
    queue: queue,
    mode: mode,
    to: normalizeEmailAddress_(queue.to_email),
    subject: String(queue.subject || ''),
    body: String(queue.body_template || '') +
      (queue.addendum ? '\n\n■ 担当者追記\n' + String(queue.addendum) : ''),
    options: {}
  };
  if (queue.cc_email) resolved.options.cc = String(queue.cc_email);
  if (queue.bcc_email) resolved.options.bcc = String(queue.bcc_email);

  if (policy.internalOverride) {
    var internalRecipients = normalizeEmailRecipients_(
      getSecureSetting_('INTERNAL_TEST_RECIPIENTS')
    ).filter(isValidEmailSyntax_);
    if (internalRecipients.length === 0) {
      throw appError_('TEST_RECIPIENT_REQUIRED', '内部テスト宛先が未設定です', false);
    }
    resolved.to = internalRecipients[0];
    resolved.options = {};
    if (internalRecipients.length > 1) {
      resolved.options.cc = internalRecipients.slice(1).join(',');
    }
    resolved.subject = '【内部テスト・外部送信禁止】' + resolved.subject;
  }
  return resolved;
}

function validateSystemInternalSendPolicy_(to, options) {
  if (getNotificationMode_() !== 'INTERNAL_TEST') {
    throw appError_('MODE_BLOCKED', '内部テストmode以外ではキュー外送信できません', false);
  }
  var allowed = {};
  normalizeEmailRecipients_(getSecureSetting_('INTERNAL_TEST_RECIPIENTS'))
    .filter(isValidEmailSyntax_)
    .forEach(function(email) { allowed[normalizeEmailAddress_(email)] = true; });
  var recipients = normalizeEmailRecipients_(to).concat(
    normalizeEmailRecipients_(options && options.cc),
    normalizeEmailRecipients_(options && options.bcc)
  );
  if (recipients.length === 0 ||
      recipients.some(function(email) { return !allowed[normalizeEmailAddress_(email)]; })) {
    throw appError_('INTERNAL_RECIPIENT_ONLY', '内部テスト許可宛先以外は送信できません', false);
  }
  return true;
}

function approveAndSendNotificationCandidates_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['items'], 'notifications.approveAndSend');
  if (!Array.isArray(payload.items) || payload.items.length < 1 || payload.items.length > 10) {
    throw appError_('INVALID_SELECTION', '通知候補は1〜10件指定してください', false);
  }
  var seen = {};
  var results = [];
  payload.items.forEach(function(item, index) {
    try {
      assertOnlyKeys_(item, ['queueId', 'addendum'], 'items[' + index + ']');
      var queueId = normalizeTextInput_(item.queueId, 100, 'queueId');
      if (!queueId || seen[queueId]) {
        throw appError_('INVALID_SELECTION', 'queueIdが重複または空です', false);
      }
      seen[queueId] = true;
      var queue = getQueueById_(queueId);
      if (!queue) throw appError_('NOT_FOUND', '通知候補が見つかりません', false);
      if (String(queue.status || '') !== 'READY') {
        throw appError_('QUEUE_STATE_CHANGED', 'READY以外の候補は送信できません', false);
      }
      var addendum = normalizeQueueAddendum_(item.addendum);
      updateRecord_(SHEETS.NotificationQueue, queue._row, {
        addendum: addendum,
        status: 'APPROVED',
        approved_at: getNowString_(),
        approved_by: user.email,
        updated_at: getNowString_(),
        error_code: '',
        error_message: ''
      });
      SpreadsheetApp.flush();

      var prepared = validateQueuedSendPolicy_({
        queueId: queueId,
        origin: 'MANUAL',
        actorEmail: user.email
      });
      var sendResult = sendSystemEmail_({
        to: prepared.to,
        subject: prepared.subject,
        body: prepared.body,
        options: prepared.options,
        notification: {
          company_id: queue.company_id,
          permit_id: queue.permit_id,
          to_email: prepared.to,
          cc_email: prepared.options.cc || '',
          bcc_email: prepared.options.bcc || '',
          stage: queue.stage,
          subject: prepared.subject,
          body: prepared.body,
          queue_id: queueId,
          idempotency_key: queue.idempotency_key,
          initiated_by: user.email,
          send_origin: 'MANUAL',
          notification_mode: prepared.mode
        },
        queueContext: {
          queueId: queueId,
          origin: 'MANUAL',
          actorEmail: user.email
        }
      });
      finalizeQueueFromSendResult_(queueId, sendResult, 'MANUAL');
      results.push({
        queueId: queueId,
        ok: sendResult.sent === true,
        result: sendResult.result,
        notificationId: sendResult.notificationId || ''
      });
    } catch (error) {
      var failedId = item && String(item.queueId || '');
      if (failedId) {
        var failedQueue = getQueueById_(failedId);
        if (failedQueue && String(failedQueue.status || '') !== 'SENT') {
          updateRecord_(SHEETS.NotificationQueue, failedQueue._row, {
            status: 'BLOCKED',
            updated_at: getNowString_(),
            error_code: String(error.code || 'SEND_FAILED'),
            error_message: String(error.message || error).substring(0, 500)
          });
        }
      }
      results.push({
        queueId: failedId,
        ok: false,
        result: String(error.code || 'SEND_FAILED'),
        message: String(error.message || '送信できませんでした')
      });
    }
  });

  appendAuditEvent_({
    user_email: user.email,
    actor_role: user.role,
    action: 'APPROVE_AND_SEND_NOTIFICATIONS',
    target_type: 'NotificationQueue',
    target_id: results.map(function(result) { return result.queueId; }).join(','),
    request_id: requestId,
    details: JSON.stringify(results),
    status: results.some(function(result) { return !result.ok; }) ? 'PARTIAL' : 'COMMITTED'
  });
  return { items: results };
}

function finalizeQueueFromSendResult_(queueId, sendResult, origin) {
  var queue = getQueueById_(queueId);
  if (!queue) return;
  var result = sendResult || {};
  var status = result.sent
    ? (result.logUpdated === false ? 'PENDING_RECONCILIATION' : 'SENT')
    : (result.result === 'FAILED' ? 'FAILED' : 'BLOCKED');
  updateRecord_(SHEETS.NotificationQueue, queue._row, {
    status: status,
    send_origin: origin,
    sent_at: result.sent ? getNowString_() : '',
    updated_at: getNowString_(),
    notification_id: result.notificationId || '',
    error_code: result.sent && result.logUpdated !== false ? '' : String(result.result || ''),
    error_message: result.sent && result.logUpdated !== false
      ? ''
      : String(result.message || result.logError_ || '').substring(0, 500)
  });
  SpreadsheetApp.flush();
}

function cancelNotificationCandidate_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['queueId', 'reason'], 'notifications.cancel');
  var queue = getQueueById_(payload.queueId);
  var reason = normalizeTextInput_(payload.reason, 200, '取消理由');
  if (!queue) throw appError_('NOT_FOUND', '通知候補が見つかりません', false);
  if (['SENT', 'SENDING', 'PENDING_RECONCILIATION'].indexOf(String(queue.status || '')) >= 0) {
    throw appError_('NOT_CANCELLABLE', '送信済み・送信中の候補は取消できません', false);
  }
  updateRecord_(SHEETS.NotificationQueue, queue._row, {
    status: 'CANCELLED',
    cancelled_at: getNowString_(),
    updated_at: getNowString_(),
    error_code: 'CANCELLED_BY_USER',
    error_message: reason
  });
  var audit = appendAuditEvent_({
    user_email: user.email,
    actor_role: user.role,
    action: 'CANCEL_NOTIFICATION',
    target_type: 'NotificationQueue',
    target_id: queue.queue_id,
    request_id: requestId,
    reason_note: reason,
    status: 'COMMITTED'
  });
  return { queueId: queue.queue_id, auditId: audit.log_id };
}

function regenerateNotificationCandidate_(payload, user, requestId) {
  assertOnlyKeys_(payload, ['queueId'], 'notifications.regenerate');
  var queue = getQueueById_(payload.queueId);
  if (!queue) throw appError_('NOT_FOUND', '通知候補が見つかりません', false);
  if (['SENT', 'SENDING', 'PENDING_RECONCILIATION'].indexOf(String(queue.status || '')) >= 0) {
    throw appError_('NOT_REGENERATABLE', '送信済み・送信中の候補は再生成できません', false);
  }
  var permit = findByKey_(SHEETS.Permits, 'permit_id', queue.permit_id);
  var company = findByKey_(SHEETS.Companies, 'company_id', queue.company_id);
  if (!permit || !company) throw appError_('NOT_FOUND', '会社または許可情報が見つかりません', false);
  updateRecord_(SHEETS.NotificationQueue, queue._row, {
    status: 'CANCELLED',
    cancelled_at: getNowString_(),
    updated_at: getNowString_(),
    error_code: 'REGENERATED',
    error_message: ''
  });
  var outcome = createOrRefreshQueueCandidate_(permit, company, queue.stage, user.email);
  var audit = appendAuditEvent_({
    user_email: user.email,
    actor_role: user.role,
    action: 'REGENERATE_NOTIFICATION',
    target_type: 'NotificationQueue',
    target_id: outcome.record.queue_id,
    request_id: requestId,
    details: 'source_queue_id=' + queue.queue_id,
    status: 'COMMITTED'
  });
  return { item: serializeQueueItem_(outcome.record), auditId: audit.log_id };
}

function countBusinessDaysSince_(dateValue) {
  var start = new Date(dateValue);
  if (isNaN(start.getTime())) return 0;
  var end = new Date();
  start.setHours(0, 0, 0, 0);
  end.setHours(0, 0, 0, 0);
  var count = 0;
  for (var cursor = new Date(start); cursor < end; cursor.setDate(cursor.getDate() + 1)) {
    var day = cursor.getDay();
    if (day !== 0 && day !== 6) count++;
  }
  return count;
}

function getModePromotionEvidence_() {
  var changedAt = getSecureSetting_('NOTIFICATION_MODE_CHANGED_AT');
  var businessDays = countBusinessDaysSince_(changedAt);
  var changedDate = new Date(changedAt);
  var sentCount = readRecords_(SHEETS.Notifications).filter(function(notification) {
    var sentAt = new Date(notification.sent_at);
    return String(notification.result || '') === 'SENT' &&
      !isNaN(sentAt.getTime()) &&
      !isNaN(changedDate.getTime()) &&
      sentAt >= changedDate;
  }).length;
  var unresolved = readRecords_(SHEETS.NotificationQueue).filter(function(queue) {
    return ['PENDING_RECONCILIATION', 'FAILED', 'STALE'].indexOf(
      String(queue.status || '')
    ) >= 0;
  }).length;
  return {
    businessDays: businessDays,
    sentCount: sentCount,
    unresolvedCount: unresolved,
    eligible: businessDays >= 10 && sentCount >= 5 && unresolved === 0
  };
}

function setNotificationModeSecure_(payload, user, requestId) {
  assertOnlyKeys_(
    payload,
    ['mode', 'reason', 'confirmNoIncidents', 'confirmReconciled'],
    'operations.setNotificationMode'
  );
  var requested = String(payload.mode || '').trim().toUpperCase();
  var reason = normalizeTextInput_(payload.reason, 200, '変更理由');
  if (NOTIFICATION_MODES_.indexOf(requested) < 0 || !reason) {
    throw appError_('INVALID_MODE', '有効なmodeと変更理由が必要です', false);
  }
  var current = getNotificationMode_();
  var currentIndex = NOTIFICATION_MODES_.indexOf(current);
  var requestedIndex = NOTIFICATION_MODES_.indexOf(requested);
  if (requested === current) return { mode: current, changed: false };

  var evidence = getModePromotionEvidence_();
  if (requested !== 'OFF' && requestedIndex > currentIndex) {
    if (requestedIndex !== currentIndex + 1) {
      throw appError_('MODE_SEQUENCE', 'modeは1段階ずつ昇格してください', false);
    }
    if (current !== 'OFF' &&
        (!evidence.eligible ||
         payload.confirmNoIncidents !== true ||
         payload.confirmReconciled !== true)) {
      throw appError_('PROMOTION_GATE_NOT_MET', '昇格条件を満たしていません', false);
    }
  }

  var now = getNowString_();
  setSecureSetting_('NOTIFICATION_MODE', requested);
  setSecureSetting_('NOTIFICATION_MODE_CHANGED_AT', now);
  setSecureSetting_('NOTIFICATION_MODE_CHANGED_BY', user.email);
  var audit = appendAuditEvent_({
    user_email: user.email,
    actor_role: user.role,
    action: 'SET_NOTIFICATION_MODE',
    target_type: 'NotificationPolicy',
    target_id: requested,
    request_id: requestId,
    reason_note: reason,
    before_json: JSON.stringify({ mode: current }),
    after_json: JSON.stringify({ mode: requested }),
    details: JSON.stringify(evidence),
    status: 'COMMITTED'
  });
  return { mode: requested, changed: true, evidence: evidence, auditId: audit.log_id };
}

function getOperationsStatus_(payload) {
  assertOnlyKeys_(payload, [], 'operations.getStatus');
  var queueRows = readRecords_(SHEETS.NotificationQueue);
  var counts = {};
  queueRows.forEach(function(row) {
    var status = String(row.status || 'UNKNOWN');
    counts[status] = (counts[status] || 0) + 1;
  });
  var companies = readRecords_(SHEETS.Companies);
  var ready = 0;
  companies.forEach(function(company) {
    if (getCompanyReadiness_(company).sendReady) ready++;
  });
  var quota = null;
  try {
    quota = Number(MailApp.getRemainingDailyQuota());
  } catch (ignored) {
    quota = null;
  }
  return {
    notificationMode: getNotificationMode_(),
    modeChangedAt: getSecureSetting_('NOTIFICATION_MODE_CHANGED_AT'),
    modeChangedBy: getSecureSetting_('NOTIFICATION_MODE_CHANGED_BY'),
    enableSend: isSendEnabled_(),
    quotaRemainingRecipients: quota,
    queueCounts: counts,
    companyCounts: {
      total: companies.length,
      sendReady: ready,
      needsSetup: companies.length - ready
    },
    promotionEvidence: getModePromotionEvidence_(),
    backup: {
      lastAt: getSecureSetting_('LAST_BACKUP_DISPLAY_AT') ||
        getSecureSetting_('LAST_BACKUP_AT'),
      lastStatus: getSecureSetting_('LAST_BACKUP_STATUS'),
      configured: !!getSecureSetting_('BACKUP_FOLDER_ID')
    },
    mlit: getMlitOperationsStatus_()
  };
}

function sendAutoEligibleCandidates_(limit) {
  var max = Number(limit || 20);
  if (!Number.isInteger(max) || max < 1) max = 20;
  max = Math.min(max, 20);
  var policy = getModePolicy_(getNotificationMode_());
  if (!policy.autoAllowed) return { attempted: 0, sent: 0, blocked: 0 };
  var rows = readRecords_(SHEETS.NotificationQueue).filter(function(queue) {
    return String(queue.status || '') === 'READY' &&
      policy.autoStages.indexOf(String(queue.stage || '')) >= 0;
  }).slice(0, max);
  var result = { attempted: rows.length, sent: 0, blocked: 0 };
  rows.forEach(function(queue) {
    try {
      var prepared = validateQueuedSendPolicy_({
        queueId: queue.queue_id,
        origin: 'AUTO',
        actorEmail: 'SYSTEM_AUTO'
      });
      var sent = sendSystemEmail_({
        to: prepared.to,
        subject: prepared.subject,
        body: prepared.body,
        options: prepared.options,
        notification: {
          company_id: queue.company_id,
          permit_id: queue.permit_id,
          to_email: prepared.to,
          cc_email: prepared.options.cc || '',
          bcc_email: prepared.options.bcc || '',
          stage: queue.stage,
          subject: prepared.subject,
          body: prepared.body,
          queue_id: queue.queue_id,
          idempotency_key: queue.idempotency_key,
          initiated_by: 'SYSTEM_AUTO',
          send_origin: 'AUTO',
          notification_mode: prepared.mode
        },
        queueContext: {
          queueId: queue.queue_id,
          origin: 'AUTO',
          actorEmail: 'SYSTEM_AUTO'
        }
      });
      finalizeQueueFromSendResult_(queue.queue_id, sent, 'AUTO');
      if (sent.sent) result.sent++;
      else result.blocked++;
    } catch (error) {
      result.blocked++;
      var current = getQueueById_(queue.queue_id);
      if (current) {
        updateRecord_(SHEETS.NotificationQueue, current._row, {
          status: 'BLOCKED',
          error_code: String(error.code || 'AUTO_BLOCKED'),
          error_message: String(error.message || error).substring(0, 500),
          updated_at: getNowString_()
        });
      }
    }
  });
  return result;
}
