/**
 * Security.gs — Google ID token 検証、UserAccess、capability制御
 *
 * ブラウザから email / role を受け取らない。本人情報は検証済みID tokenと
 * UserAccessからのみ構築し、未設定・検証失敗時は必ずfail-closedとする。
 */

var APP_ROLES_ = [
  'master_editor',
  'operations_admin',
  'technical_admin',
  'legacy_admin'
];

var ROLE_CAPABILITIES_ = {
  master_editor: [
    'session.read', 'dashboard.read',
    'companies.read', 'companies.edit_contacts',
    'notifications.review', 'notifications.send',
    'mlit.read', 'mlit.request_refresh', 'mlit.apply_diff'
  ],
  operations_admin: [
    'session.read', 'dashboard.read',
    'companies.read', 'companies.edit_contacts', 'companies.revert',
    'notifications.review', 'notifications.send', 'notifications.reconcile',
    'mlit.read', 'mlit.request_refresh', 'mlit.apply_diff', 'mlit.configure',
    'companies.manage_monitoring',
    'operations.read', 'operations.write',
    'users.read', 'users.write', 'schema.migrate'
  ],
  technical_admin: [
    'session.read', 'dashboard.read', 'companies.read',
    'notifications.review', 'mlit.read', 'mlit.request_refresh',
    'operations.read', 'schema.migrate'
  ],
  // Phase 0-Cの非公開移行時だけ使用する後方互換role。公開前に必ず解消する。
  legacy_admin: [
    'session.read', 'dashboard.read',
    'companies.read', 'companies.edit_contacts', 'companies.revert',
    'notifications.review', 'notifications.send', 'notifications.reconcile',
    'mlit.read', 'mlit.request_refresh', 'mlit.apply_diff', 'mlit.configure',
    'companies.manage_monitoring',
    'operations.read', 'operations.write',
    'users.read', 'users.write', 'schema.migrate'
  ]
};

function appError_(code, message, retryable, auditId) {
  var error = new Error(message || code || 'APP_ERROR');
  error.code = String(code || 'APP_ERROR');
  error.retryable = retryable === true;
  error.auditId = String(auditId || '');
  return error;
}

function normalizeEmailAddress_(value) {
  return String(value || '').trim().toLowerCase();
}

function isValidEmailSyntax_(value) {
  var email = normalizeEmailAddress_(value);
  return email.length <= 254 &&
    email.indexOf('\r') < 0 &&
    email.indexOf('\n') < 0 &&
    /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/i.test(email);
}

function parseStrictBoolean_(value) {
  return value === true || String(value || '').trim().toLowerCase() === 'true';
}

function getScriptProperties_() {
  return PropertiesService.getScriptProperties();
}

function getSecureSetting_(key) {
  var value = getScriptProperties_().getProperty(String(key || ''));
  if (value !== null && value !== undefined) return String(value);

  // OAuth client IDは秘密情報ではないため旧Configからの移行期間だけ読取を許可する。
  if (key === 'GOOGLE_CLIENT_ID') {
    try {
      return String(getConfig_(key) || '');
    } catch (ignored) {
      return '';
    }
  }
  return '';
}

function setSecureSetting_(key, value) {
  getScriptProperties_().setProperty(String(key), String(value));
}

function sha256Hex_(value) {
  var bytes = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    String(value || ''),
    Utilities.Charset.UTF_8
  );
  return bytes.map(function(byte) {
    var unsigned = byte < 0 ? byte + 256 : byte;
    return ('0' + unsigned.toString(16)).slice(-2);
  }).join('');
}

function getTokenCache_() {
  try {
    return CacheService.getScriptCache();
  } catch (ignored) {
    return null;
  }
}

/**
 * Google tokeninfoでID tokenの署名・claimsを検証する。
 * @param {string} idToken
 * @return {Object}
 */
function verifyGoogleIdToken_(idToken) {
  var token = String(idToken || '').trim();
  if (!token || token.length < 100 || token.length > 10000 || token.split('.').length !== 3) {
    throw appError_('UNAUTHORIZED', 'Google認証情報がありません', false);
  }

  var clientId = String(getSecureSetting_('GOOGLE_CLIENT_ID') || '').trim();
  if (!clientId) {
    throw appError_('AUTH_NOT_CONFIGURED', 'Google認証のclient IDが未設定です', false);
  }

  var cache = getTokenCache_();
  var cacheKey = 'gid:' + sha256Hex_(token);
  if (cache) {
    var cached = cache.get(cacheKey);
    if (cached) {
      try {
        var cachedClaims = JSON.parse(cached);
        if (Number(cachedClaims.exp || 0) > Math.floor(Date.now() / 1000) + 10) {
          return cachedClaims;
        }
      } catch (ignoredCacheError) {
        // tokeninfoで再検証する。
      }
    }
  }

  var response;
  try {
    response = UrlFetchApp.fetch(
      'https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(token),
      {
        method: 'get',
        muteHttpExceptions: true,
        followRedirects: false
      }
    );
  } catch (fetchError) {
    throw appError_('AUTH_PROVIDER_UNAVAILABLE', 'Google認証を確認できません', true);
  }

  if (!response || response.getResponseCode() !== 200) {
    throw appError_('UNAUTHORIZED', 'Google認証情報が無効です', false);
  }

  var claims;
  try {
    claims = JSON.parse(response.getContentText());
  } catch (parseError) {
    throw appError_('UNAUTHORIZED', 'Google認証応答を検証できません', false);
  }

  var nowSeconds = Math.floor(Date.now() / 1000);
  var issuer = String(claims.iss || '');
  var validIssuer = issuer === 'accounts.google.com' || issuer === 'https://accounts.google.com';
  var verified = claims.email_verified === true ||
    String(claims.email_verified || '').toLowerCase() === 'true';

  if (String(claims.aud || '') !== clientId ||
      !validIssuer ||
      Number(claims.exp || 0) <= nowSeconds ||
      !verified ||
      !normalizeEmailAddress_(claims.email)) {
    throw appError_('UNAUTHORIZED', 'Google認証claimが無効です', false);
  }

  var safeClaims = {
    sub: String(claims.sub || ''),
    email: normalizeEmailAddress_(claims.email),
    name: String(claims.name || ''),
    aud: String(claims.aud || ''),
    iss: issuer,
    exp: Number(claims.exp)
  };
  if (cache) {
    var ttl = Math.max(1, Math.min(300, safeClaims.exp - nowSeconds - 5));
    cache.put(cacheKey, JSON.stringify(safeClaims), ttl);
  }
  return safeClaims;
}

function normalizeRole_(role) {
  var normalized = String(role || '').trim().toLowerCase();
  if (normalized === 'admin') return 'legacy_admin';
  return APP_ROLES_.indexOf(normalized) >= 0 ? normalized : '';
}

function getUserAccessByEmail_(email) {
  var target = normalizeEmailAddress_(email);
  if (!target) return null;
  var records = readRecords_(SHEETS.UserAccess);
  for (var i = 0; i < records.length; i++) {
    if (normalizeEmailAddress_(records[i].email) !== target) continue;
    var role = normalizeRole_(records[i].role);
    return {
      email: target,
      role: role,
      active: parseStrictBoolean_(records[i].active),
      displayName: String(records[i].displayName || ''),
      managerEmail: normalizeEmailAddress_(records[i].managerEmail),
      canSendExternal: parseStrictBoolean_(records[i].canSendExternal),
      _row: records[i]._row
    };
  }
  return null;
}

function buildAuthenticatedUser_(idToken) {
  var claims = verifyGoogleIdToken_(idToken);
  var access = getUserAccessByEmail_(claims.email);
  if (!access || !access.active || !access.role) {
    writeSecurityAuditBestEffort_(
      claims.email,
      'AUTH_DENIED',
      !access ? 'USER_NOT_REGISTERED' : (!access.active ? 'USER_INACTIVE' : 'ROLE_INVALID')
    );
    throw appError_('FORBIDDEN', 'このシステムの利用権限がありません', false);
  }
  return {
    email: access.email,
    role: access.role,
    displayName: access.displayName || claims.name || access.email.split('@')[0],
    managerEmail: access.managerEmail,
    canSendExternal: access.canSendExternal,
    capabilities: (ROLE_CAPABILITIES_[access.role] || []).slice()
  };
}

function hasCapability_(user, capability) {
  return !!user &&
    Array.isArray(user.capabilities) &&
    user.capabilities.indexOf(String(capability || '')) >= 0;
}

function requireCapability_(user, capability) {
  if (!hasCapability_(user, capability)) {
    writeSecurityAuditBestEffort_(
      user && user.email,
      'CAPABILITY_DENIED',
      String(capability || '')
    );
    throw appError_('FORBIDDEN', 'この操作を実行する権限がありません', false);
  }
  return user;
}

function requireExternalSendCapability_(user) {
  requireCapability_(user, 'notifications.send');
  if (!user.canSendExternal) {
    throw appError_('FORBIDDEN_SEND', '外部送信担当者として登録されていません', false);
  }
  return user;
}

function writeSecurityAuditBestEffort_(email, action, detail) {
  try {
    appendAuditEvent_({
      user_email: normalizeEmailAddress_(email),
      action: action,
      target_type: 'Security',
      target_id: '',
      details: String(detail || '').substring(0, 500),
      status: 'COMMITTED'
    });
  } catch (auditError) {
    console.error('[SECURITY_AUDIT_FAILED] ' + String(auditError.message || auditError));
  }
}

function publicUser_(user) {
  return {
    email: user.email,
    role: user.role,
    displayName: user.displayName,
    managerEmail: user.managerEmail,
    canSendExternal: user.canSendExternal,
    capabilities: user.capabilities.slice()
  };
}
