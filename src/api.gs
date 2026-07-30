/**
 * api.gs — google.script.runから呼べる唯一の業務API
 */

var API_ACTIONS_ = {
  'session.get': { capability: 'session.read', handler: apiSessionGet_ },
  'dashboard.get': { capability: 'dashboard.read', handler: apiDashboardGet_ },
  'companies.list': { capability: 'companies.read', handler: apiCompaniesList_ },
  'companies.get': { capability: 'companies.read', handler: apiCompaniesGet_ },
  'companies.updateContacts': {
    capability: 'companies.edit_contacts',
    handler: apiCompaniesUpdateContacts_
  },
  'companies.revertChange': {
    capability: 'companies.revert',
    handler: apiCompaniesRevertChange_
  },
  'notifications.listCandidates': {
    capability: 'notifications.review',
    handler: apiNotificationsListCandidates_
  },
  'notifications.preview': {
    capability: 'notifications.review',
    handler: apiNotificationsPreview_
  },
  'notifications.approveAndSend': {
    capability: 'notifications.send',
    handler: apiNotificationsApproveAndSend_
  },
  'notifications.cancel': {
    capability: 'notifications.review',
    handler: apiNotificationsCancel_
  },
  'notifications.regenerate': {
    capability: 'notifications.review',
    handler: apiNotificationsRegenerate_
  },
  'operations.getStatus': {
    capability: 'operations.read',
    handler: apiOperationsGetStatus_
  },
  'operations.setNotificationMode': {
    capability: 'operations.write',
    handler: apiOperationsSetNotificationMode_
  },
  'operations.ensureSchema': {
    capability: 'schema.migrate',
    handler: apiOperationsEnsureSchema_
  },
  'users.list': { capability: 'users.read', handler: apiUsersList_ },
  'users.update': { capability: 'users.write', handler: apiUsersUpdate_ }
};

function isPlainObject_(value) {
  return value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.prototype.toString.call(value) === '[object Object]';
}

function assertOnlyKeys_(object, allowedKeys, label) {
  if (!isPlainObject_(object)) {
    throw appError_('INVALID_PAYLOAD', (label || 'payload') + 'はobjectで指定してください', false);
  }
  var allowed = {};
  allowedKeys.forEach(function(key) { allowed[key] = true; });
  Object.keys(object).forEach(function(key) {
    if (!allowed[key]) {
      throw appError_(
        'INVALID_PAYLOAD',
        (label || 'payload') + 'に未許可の項目があります: ' + key,
        false
      );
    }
  });
}

function normalizeApiRequest_(request) {
  assertOnlyKeys_(request, ['version', 'requestId', 'action', 'payload'], 'request');
  if (String(request.version || '') !== '1') {
    throw appError_('UNSUPPORTED_VERSION', 'API versionが不正です', false);
  }
  var requestId = String(request.requestId || '').trim();
  if (!/^[A-Za-z0-9_-]{8,100}$/.test(requestId)) {
    throw appError_('INVALID_REQUEST_ID', 'requestIdが不正です', false);
  }
  var action = String(request.action || '').trim();
  if (!API_ACTIONS_[action]) {
    throw appError_('UNKNOWN_ACTION', '未定義の操作です', false);
  }
  var payload = request.payload === undefined ? {} : request.payload;
  if (!isPlainObject_(payload)) {
    throw appError_('INVALID_PAYLOAD', 'payloadはobjectで指定してください', false);
  }
  return {
    version: '1',
    requestId: requestId,
    action: action,
    payload: payload
  };
}

/**
 * @param {Object} request
 * @param {string} idToken
 * @return {Object}
 */
function apiDispatch(request, idToken) {
  var normalized = null;
  var user = null;
  try {
    normalized = normalizeApiRequest_(request);
    user = buildAuthenticatedUser_(idToken);
    var route = API_ACTIONS_[normalized.action];
    requireCapability_(user, route.capability);
    var data = route.handler(normalized.payload, user, normalized.requestId);
    return toSerializable_({
      ok: true,
      requestId: normalized.requestId,
      data: data === undefined ? null : data
    });
  } catch (error) {
    var requestId = normalized ? normalized.requestId :
      (request && String(request.requestId || '').substring(0, 100)) || '';
    var code = String(error && error.code || 'INTERNAL_ERROR');
    var safeMessage = code === 'INTERNAL_ERROR'
      ? '処理中にエラーが発生しました'
      : String(error && error.message || '処理に失敗しました');

    if (code === 'INTERNAL_ERROR') {
      console.error(error && error.stack ? error.stack : error);
    }
    try {
      appendAuditEvent_({
        user_email: user && user.email,
        actor_role: user && user.role,
        action: 'API_ERROR',
        target_type: 'ApiAction',
        target_id: normalized && normalized.action,
        request_id: requestId,
        details: code,
        status: 'ABORTED',
        error_code: code
      });
    } catch (ignoredAuditError) {
      console.error('[API_AUDIT_FAILED] ' + String(ignoredAuditError.message || ignoredAuditError));
    }
    return {
      ok: false,
      requestId: requestId,
      error: {
        code: code,
        message: safeMessage,
        retryable: error && error.retryable === true,
        auditId: String(error && error.auditId || '')
      }
    };
  }
}

function apiSessionGet_(payload, user) {
  assertOnlyKeys_(payload, [], 'session.get');
  return publicUser_(user);
}

function apiDashboardGet_(payload) {
  assertOnlyKeys_(payload, [], 'dashboard.get');
  return buildDashboardData_();
}

function apiCompaniesList_(payload, user) {
  return listCompaniesSecure_(payload, user);
}

function apiCompaniesGet_(payload, user) {
  return getCompanySecure_(payload, user);
}

function apiCompaniesUpdateContacts_(payload, user, requestId) {
  return updateCompanyContactsSecure_(payload, user, requestId);
}

function apiCompaniesRevertChange_(payload, user, requestId) {
  return revertCompanyChangeSecure_(payload, user, requestId);
}

function apiNotificationsListCandidates_(payload, user) {
  return listNotificationCandidates_(payload, user);
}

function apiNotificationsPreview_(payload, user) {
  return previewNotificationCandidates_(payload, user);
}

function apiNotificationsApproveAndSend_(payload, user, requestId) {
  requireExternalSendCapability_(user);
  return approveAndSendNotificationCandidates_(payload, user, requestId);
}

function apiNotificationsCancel_(payload, user, requestId) {
  return cancelNotificationCandidate_(payload, user, requestId);
}

function apiNotificationsRegenerate_(payload, user, requestId) {
  return regenerateNotificationCandidate_(payload, user, requestId);
}

function apiOperationsGetStatus_(payload, user) {
  return getOperationsStatus_(payload, user);
}

function apiOperationsSetNotificationMode_(payload, user, requestId) {
  return setNotificationModeSecure_(payload, user, requestId);
}

function apiOperationsEnsureSchema_(payload, user, requestId) {
  return ensureApplicationSchemaSecure_(payload, user, requestId);
}

function apiUsersList_(payload, user) {
  return listUsersSecure_(payload, user);
}

function apiUsersUpdate_(payload, user, requestId) {
  return updateUserSecure_(payload, user, requestId);
}
