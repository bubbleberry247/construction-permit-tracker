/**
 * OAuthLogin.gs — Apps Script HTML Service向けGoogle OIDC認証
 *
 * HTML Serviceはgoogleusercontent.comのsandbox iframeで動作し、そのGoogle所有
 * originはOAuth clientのAuthorized JavaScript originsへ登録できない。このため
 * GISのブラウザ内callbackは使わず、Authorization Code + PKCEを使用する。
 *
 * 認証開始状態とID token受渡しは短時間のScript Cacheだけに保持する。
 * tokenは一度pollされた時点で削除し、スプレッドシートやログへ保存しない。
 */

var OAUTH_FLOW_CACHE_PREFIX_ = 'oidc_flow:';
var OAUTH_FLOW_TTL_SECONDS_ = 600;
var OAUTH_RESULT_TTL_SECONDS_ = 180;
var OAUTH_MAX_POLL_COUNT_ = 120;
var OAUTH_MAX_STARTS_PER_MINUTE_ = 30;

function getOAuthFlowCache_() {
  var cache = getTokenCache_();
  if (!cache || typeof cache.get !== 'function' ||
      typeof cache.put !== 'function' ||
      typeof cache.remove !== 'function') {
    throw appError_('AUTH_PROVIDER_UNAVAILABLE', 'Google認証を開始できません', true);
  }
  return cache;
}

function normalizeOAuthState_(value) {
  var state = String(value || '').trim();
  if (!/^[A-Za-z0-9_-]{48,160}$/.test(state)) {
    throw appError_('AUTH_FLOW_INVALID', '認証要求が不正です', false);
  }
  return state;
}

function oauthFlowCacheKey_(state) {
  return OAUTH_FLOW_CACHE_PREFIX_ + sha256Hex_(normalizeOAuthState_(state));
}

function createOAuthRandomValue_() {
  var value = '';
  for (var i = 0; i < 3; i++) {
    value += String(Utilities.getUuid() || '').replace(/[^A-Za-z0-9]/g, '');
  }
  if (value.length < 64) {
    throw appError_('AUTH_PROVIDER_UNAVAILABLE', 'Google認証を開始できません', true);
  }
  return value.substring(0, 96);
}

function base64UrlEncodeBytes_(bytes) {
  return String(Utilities.base64EncodeWebSafe(bytes) || '').replace(/=+$/g, '');
}

function getOAuthRedirectUri_() {
  var uri = String(getSecureSetting_('GOOGLE_OAUTH_REDIRECT_URI') || '').trim();
  if (!/^https:\/\/script\.google\.com\/macros\/s\/[A-Za-z0-9_-]+\/exec$/.test(uri)) {
    throw appError_(
      'AUTH_NOT_CONFIGURED',
      'Google認証のredirect URIが未設定です',
      false
    );
  }
  return uri;
}

function assertOAuthServerConfiguration_() {
  var clientId = String(getSecureSetting_('GOOGLE_CLIENT_ID') || '').trim();
  var clientSecret = String(getSecureSetting_('GOOGLE_CLIENT_SECRET') || '').trim();
  var redirectUri = getOAuthRedirectUri_();
  if (!/^[A-Za-z0-9._-]+\.apps\.googleusercontent\.com$/.test(clientId) ||
      clientSecret.length < 20 ||
      clientSecret.length > 500) {
    throw appError_('AUTH_NOT_CONFIGURED', 'Google認証の設定が未完了です', false);
  }
  return {
    clientId: clientId,
    clientSecret: clientSecret,
    redirectUri: redirectUri
  };
}

function buildGoogleAuthorizationUrl_(config, state, codeChallenge) {
  var query = {
    client_id: config.clientId,
    redirect_uri: config.redirectUri,
    response_type: 'code',
    scope: 'openid email profile',
    state: state,
    code_challenge: codeChallenge,
    code_challenge_method: 'S256',
    access_type: 'online',
    prompt: 'select_account',
    include_granted_scopes: 'false'
  };
  var parts = Object.keys(query).map(function(key) {
    return encodeURIComponent(key) + '=' + encodeURIComponent(String(query[key]));
  });
  return 'https://accounts.google.com/o/oauth2/v2/auth?' + parts.join('&');
}

function enforceOAuthStartRateLimit_() {
  var cache = getOAuthFlowCache_();
  var bucket = Math.floor(Date.now() / 60000);
  var key = OAUTH_FLOW_CACHE_PREFIX_ + 'rate:' + bucket;
  withOAuthFlowLock_(function() {
    var count = Number(cache.get(key) || 0);
    if (!Number.isFinite(count) || count < 0) count = 0;
    if (count >= OAUTH_MAX_STARTS_PER_MINUTE_) {
      throw appError_(
        'AUTH_RATE_LIMITED',
        '認証開始が集中しています。しばらく待って再試行してください',
        true
      );
    }
    cache.put(key, String(count + 1), 120);
  });
}

function apiAuthStart_(payload, unusedUser, requestId) {
  assertOnlyKeys_(payload, [], 'auth.start');
  var config = assertOAuthServerConfiguration_();
  enforceOAuthStartRateLimit_();
  var state = createOAuthRandomValue_();
  var verifier = createOAuthRandomValue_();
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    verifier,
    Utilities.Charset.UTF_8
  );
  var challenge = base64UrlEncodeBytes_(digest);
  if (!challenge) {
    throw appError_('AUTH_PROVIDER_UNAVAILABLE', 'Google認証を開始できません', true);
  }

  var cache = getOAuthFlowCache_();
  cache.put(oauthFlowCacheKey_(state), JSON.stringify({
    status: 'PENDING',
    verifier: verifier,
    requestId: String(requestId || ''),
    pollCount: 0,
    createdAt: new Date().toISOString()
  }), OAUTH_FLOW_TTL_SECONDS_);

  return {
    state: state,
    authorizationUrl: buildGoogleAuthorizationUrl_(config, state, challenge),
    expiresInSeconds: OAUTH_FLOW_TTL_SECONDS_
  };
}

function readOAuthFlow_(state) {
  var normalizedState = normalizeOAuthState_(state);
  var cache = getOAuthFlowCache_();
  var raw = cache.get(oauthFlowCacheKey_(normalizedState));
  if (!raw) {
    throw appError_('AUTH_FLOW_EXPIRED', '認証要求の有効期限が切れました', false);
  }
  var flow;
  try {
    flow = JSON.parse(raw);
  } catch (error) {
    cache.remove(oauthFlowCacheKey_(normalizedState));
    throw appError_('AUTH_FLOW_INVALID', '認証要求を確認できません', false);
  }
  return {
    state: normalizedState,
    cache: cache,
    key: oauthFlowCacheKey_(normalizedState),
    flow: flow
  };
}

function withOAuthFlowLock_(callback) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    throw appError_('AUTH_FLOW_BUSY', '認証結果を処理中です', true);
  }
  try {
    return callback();
  } finally {
    lock.releaseLock();
  }
}

function apiAuthPoll_(payload) {
  assertOnlyKeys_(payload, ['state'], 'auth.poll');
  return withOAuthFlowLock_(function() {
    var current = readOAuthFlow_(payload.state);
    var flow = current.flow;

    if (flow.status === 'COMPLETED') {
      var token = String(flow.idToken || '');
      current.cache.remove(current.key);
      if (!token) {
        throw appError_('AUTH_FLOW_INVALID', 'Google認証結果を確認できません', false);
      }
      return {
        status: 'COMPLETED',
        credential: token
      };
    }

    if (flow.status === 'ERROR') {
      current.cache.remove(current.key);
      throw appError_(
        String(flow.errorCode || 'AUTH_FAILED'),
        'Google認証に失敗しました。もう一度お試しください',
        false
      );
    }

    if (flow.status !== 'PENDING' && flow.status !== 'EXCHANGING') {
      current.cache.remove(current.key);
      throw appError_('AUTH_FLOW_INVALID', 'Google認証結果を確認できません', false);
    }

    var pollCount = Number(flow.pollCount || 0) + 1;
    if (pollCount > OAUTH_MAX_POLL_COUNT_) {
      current.cache.remove(current.key);
      throw appError_('AUTH_FLOW_EXPIRED', '認証確認がタイムアウトしました', false);
    }
    flow.pollCount = pollCount;
    current.cache.put(current.key, JSON.stringify(flow), OAUTH_FLOW_TTL_SECONDS_);
    return { status: 'PENDING' };
  });
}

function isOAuthCallbackRequest_(event) {
  var parameters = event && event.parameter ? event.parameter : {};
  return !!String(parameters.state || '').trim() &&
    (!!String(parameters.code || '').trim() || !!String(parameters.error || '').trim());
}

function claimOAuthFlowForCallback_(state) {
  return withOAuthFlowLock_(function() {
    var current = readOAuthFlow_(state);
    if (current.flow.status !== 'PENDING') {
      throw appError_('AUTH_FLOW_REUSED', 'この認証要求はすでに処理されています', false);
    }
    current.flow.status = 'EXCHANGING';
    current.cache.put(
      current.key,
      JSON.stringify(current.flow),
      OAUTH_FLOW_TTL_SECONDS_
    );
    return current;
  });
}

function storeOAuthFlowResult_(state, result) {
  return withOAuthFlowLock_(function() {
    var current = readOAuthFlow_(state);
    if (current.flow.status !== 'EXCHANGING') {
      throw appError_('AUTH_FLOW_REUSED', 'この認証要求はすでに処理されています', false);
    }
    var stored = result || {};
    stored.pollCount = Number(current.flow.pollCount || 0);
    stored.completedAt = new Date().toISOString();
    current.cache.put(
      current.key,
      JSON.stringify(stored),
      OAUTH_RESULT_TTL_SECONDS_
    );
    return stored;
  });
}

function exchangeGoogleAuthorizationCode_(code, verifier) {
  var authorizationCode = String(code || '').trim();
  var codeVerifier = String(verifier || '').trim();
  if (!authorizationCode || authorizationCode.length > 4096 ||
      !/^[A-Za-z0-9_-]{43,128}$/.test(codeVerifier)) {
    throw appError_('AUTH_FLOW_INVALID', 'Google認証応答が不正です', false);
  }
  var config = assertOAuthServerConfiguration_();
  var response;
  try {
    response = UrlFetchApp.fetch('https://oauth2.googleapis.com/token', {
      method: 'post',
      contentType: 'application/x-www-form-urlencoded',
      payload: {
        code: authorizationCode,
        client_id: config.clientId,
        client_secret: config.clientSecret,
        redirect_uri: config.redirectUri,
        grant_type: 'authorization_code',
        code_verifier: codeVerifier
      },
      muteHttpExceptions: true,
      followRedirects: false
    });
  } catch (error) {
    throw appError_('AUTH_PROVIDER_UNAVAILABLE', 'Google認証を確認できません', true);
  }
  if (!response || response.getResponseCode() !== 200) {
    throw appError_('UNAUTHORIZED', 'Google認証応答が無効です', false);
  }
  var tokenResponse;
  try {
    tokenResponse = JSON.parse(response.getContentText());
  } catch (error) {
    throw appError_('UNAUTHORIZED', 'Google認証応答を確認できません', false);
  }
  var idToken = String(tokenResponse.id_token || '').trim();
  if (!idToken || idToken.length > 10000) {
    throw appError_('UNAUTHORIZED', 'Google認証情報を取得できません', false);
  }
  return idToken;
}

function completeOAuthCallback_(parameters) {
  var state = normalizeOAuthState_(parameters && parameters.state);
  var current = claimOAuthFlowForCallback_(state);
  try {
    if (String(parameters && parameters.error || '').trim()) {
      throw appError_('AUTH_CANCELLED', 'Google認証がキャンセルされました', false);
    }
    var idToken = exchangeGoogleAuthorizationCode_(
      parameters && parameters.code,
      current.flow.verifier
    );
    var user = buildAuthenticatedUser_(idToken);
    storeOAuthFlowResult_(state, {
      status: 'COMPLETED',
      idToken: idToken,
      email: user.email
    });
    writeSecurityAuditBestEffort_(user.email, 'AUTH_SUCCEEDED', 'OIDC_CODE_PKCE');
    return { ok: true, email: user.email };
  } catch (error) {
    try {
      storeOAuthFlowResult_(state, {
        status: 'ERROR',
        errorCode: String(error && error.code || 'AUTH_FAILED')
      });
    } catch (ignoredStoreError) {
      // EXCHANGINGのまま期限切れになる。tokenは返さない。
    }
    writeSecurityAuditBestEffort_(
      '',
      'AUTH_CALLBACK_FAILED',
      String(error && error.code || 'AUTH_FAILED')
    );
    throw error;
  }
}

function escapeOAuthResultText_(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderOAuthCallbackResult_(success, message) {
  var title = success ? 'Google認証が完了しました' : 'Google認証に失敗しました';
  var safeTitle = escapeOAuthResultText_(title);
  var safeMessage = escapeOAuthResultText_(message);
  return HtmlService.createHtmlOutput(
    '<!doctype html><html lang="ja"><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>' + safeTitle + '</title></head><body>' +
    '<main style="font-family:sans-serif;max-width:32rem;margin:4rem auto;padding:1.5rem">' +
    '<h1 style="font-size:1.35rem">' + safeTitle + '</h1>' +
    '<p>' + safeMessage + '</p>' +
    '<p>元のシステム画面へ戻ってください。この画面は閉じて構いません。</p>' +
    '</main></body></html>'
  ).setTitle(title)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function handleOAuthCallback_(event) {
  try {
    completeOAuthCallback_(event && event.parameter ? event.parameter : {});
    return renderOAuthCallbackResult_(
      true,
      '利用権限を確認しました。元の画面で自動的にログインします。'
    );
  } catch (error) {
    return renderOAuthCallbackResult_(
      false,
      error && error.code === 'FORBIDDEN'
        ? 'このGoogleアカウントは利用者として登録されていません。'
        : '認証要求を確認できませんでした。元の画面からやり直してください。'
    );
  }
}
