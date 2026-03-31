/**
 * auth.gs — OAuth 2.0認証（Google Sign-In）
 *
 * KNOWN BUG FIXES APPLIED:
 *   Bug1: JSON template <?= ?> → .replace(/</g, '\\u003c') で HTML entity エスケープ防止
 *   Bug2: getUserContext_ で clientUserKey (OAuth) を Session より先にチェック
 *   Bug4: APP_EXEC_URL → ScriptApp.getService().getUrl() を一次、Config を二次
 *   Bug5: google.accounts.id.prompt() → renderButton() のみ使用（モバイル対応）
 */

// ---------------------------------------------------------------------------
// Google Login（ID Token検証）
// ---------------------------------------------------------------------------

/**
 * Google ID Token を検証してユーザー情報を返す
 * @param {string} idToken
 * @return {{ ok: boolean, email: string, displayName: string, role: string }|{ _error: boolean, message: string }}
 */
function apiLoginWithGoogle(idToken) {
  try {
    if (!idToken) return { _error: true, message: 'ID Tokenが指定されていません' };

    var url = 'https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(idToken);
    var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (response.getResponseCode() !== 200) {
      return { _error: true, message: 'Google認証に失敗しました (status=' + response.getResponseCode() + ')' };
    }

    var payload = JSON.parse(response.getContentText());
    var email = String(payload.email || '').trim().toLowerCase();
    if (!email) return { _error: true, message: 'メールアドレスを取得できませんでした' };

    // Client ID の照合
    var expectedClientId = getConfig('GOOGLE_CLIENT_ID');
    if (expectedClientId && payload.aud !== expectedClientId) {
      return { _error: true, message: 'Client IDが一致しません' };
    }

    // UserAccess ホワイトリスト確認
    var access = getUserAccessByEmail_(email);
    if (!access || String(access.active).toLowerCase() !== 'true') {
      logAuthEvent_('LOGIN_DENIED', 'email=' + email + ' not in whitelist or inactive');
      return { _error: true, message: 'このアカウントは登録されていません: ' + email };
    }

    logAuthEvent_('LOGIN_OK', 'email=' + email + ' role=' + (access.role || 'user'));
    return {
      ok: true,
      email: email,
      displayName: access.displayName || payload.name || email.split('@')[0],
      role: access.role || 'user'
    };
  } catch (e) {
    return { _error: true, message: 'Googleログインエラー: ' + String(e.message || e) };
  }
}

/**
 * Google Client ID を返す（index.html 側で使用）
 */
function apiGetGoogleClientId() {
  try {
    return { clientId: getConfig('GOOGLE_CLIENT_ID') || '' };
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// URL / OAuth State
// ---------------------------------------------------------------------------

/**
 * Webアプリの実行URLを返す（Bug4: ScriptApp優先、Config代替）
 * @return {string}
 */
function getAppExecUrl_() {
  var deployUrl = '';
  try {
    deployUrl = ScriptApp.getService().getUrl();
  } catch (e) {
    // ignore — script may not be deployed yet
  }
  var configuredUrl = getConfig('APP_EXEC_URL') || '';
  if (deployUrl) {
    if (configuredUrl && configuredUrl !== deployUrl) {
      Logger.log('[URL_MISMATCH] config=' + configuredUrl + ' deploy=' + deployUrl);
    }
    return deployUrl;
  }
  return configuredUrl;
}

/**
 * OAuth 2.0 認可URLを生成する（state/nonce を CacheService に 300s 保存）
 * @return {string}
 */
function getOAuthStartUrl_() {
  var clientId = getConfig('GOOGLE_CLIENT_ID') || '';
  var redirectUri = getAppExecUrl_();
  var state = Utilities.getUuid();
  var nonce = Utilities.getUuid();
  CacheService.getScriptCache().put('oauth_state_' + state, nonce, 300);
  return 'https://accounts.google.com/o/oauth2/v2/auth?' +
    'client_id=' + encodeURIComponent(clientId) +
    '&redirect_uri=' + encodeURIComponent(redirectUri) +
    '&response_type=code' +
    '&scope=' + encodeURIComponent('openid email profile') +
    '&state=' + encodeURIComponent(state) +
    '&nonce=' + encodeURIComponent(nonce) +
    '&prompt=select_account';
}

// ---------------------------------------------------------------------------
// HTML Page Generation
// ---------------------------------------------------------------------------

/**
 * OAuthログインページHTMLを生成する（Bug5: prompt()禁止、renderButton()のみ）
 * @return {GoogleAppsScript.HTML.HtmlOutput}
 */
function generateOAuthStartPage_() {
  var oauthUrl = getOAuthStartUrl_();
  var html =
    '<!DOCTYPE html>' +
    '<html lang="ja">' +
    '<head>' +
    '<meta charset="UTF-8">' +
    '<meta name="viewport" content="width=device-width, initial-scale=1">' +
    '<title>建設業許可管理システム — ログイン</title>' +
    '<style>' +
    'body{margin:0;font-family:"Segoe UI","Meiryo",sans-serif;background:#f5f5f5;display:flex;align-items:center;justify-content:center;min-height:100vh;}' +
    '.card{background:#fff;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.15);padding:40px 32px;max-width:360px;width:100%;text-align:center;}' +
    '.logo{font-size:20px;font-weight:700;color:#1a73e8;margin-bottom:8px;}' +
    '.subtitle{font-size:13px;color:#666;margin-bottom:32px;}' +
    '.btn-google{display:inline-flex;align-items:center;justify-content:center;gap:10px;background:#fff;color:#3c4043;border:1px solid #dadce0;border-radius:4px;padding:10px 24px;font-size:14px;font-weight:500;text-decoration:none;cursor:pointer;transition:background .2s,box-shadow .2s;width:100%;box-sizing:border-box;}' +
    '.btn-google:hover{background:#f8f9fa;box-shadow:0 1px 4px rgba(0,0,0,.2);}' +
    '.btn-google svg{flex-shrink:0;}' +
    '.note{font-size:11px;color:#999;margin-top:20px;}' +
    '</style>' +
    '</head>' +
    '<body>' +
    '<div class="card">' +
    '<div class="logo">建設業許可管理システム</div>' +
    '<div class="subtitle">Googleアカウントでログインしてください</div>' +
    '<a class="btn-google" href="' + oauthUrl + '" target="_top">' +
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48" width="20" height="20"><path fill="#EA4335" d="M24 9.5c3.2 0 5.8 1.1 7.9 2.9l5.9-5.9C34.1 3.4 29.4 1.5 24 1.5 14.8 1.5 7 7.1 3.9 14.8l6.9 5.3C12.4 13.7 17.7 9.5 24 9.5z"/><path fill="#4285F4" d="M46.5 24.5c0-1.6-.1-3.1-.4-4.5H24v8.5h12.7c-.6 3-2.3 5.5-4.8 7.2l7.4 5.7c4.3-4 6.8-9.9 6.8-16.9z"/><path fill="#FBBC05" d="M10.8 28.4c-.5-1.4-.8-2.8-.8-4.4s.3-3 .8-4.4l-6.9-5.3C2.3 17.1 1.5 20.5 1.5 24s.8 6.9 2.4 9.7l6.9-5.3z"/><path fill="#34A853" d="M24 46.5c5.4 0 9.9-1.8 13.2-4.8l-7.4-5.7c-1.9 1.3-4.3 2-5.8 2-6.3 0-11.6-4.2-13.2-9.9l-6.9 5.3C7 40.9 14.8 46.5 24 46.5z"/></svg>' +
    'Googleでログイン' +
    '</a>' +
    '<div class="note">※ 登録済みのGoogleアカウントのみアクセスできます</div>' +
    '</div>' +
    '</body>' +
    '</html>';

  return HtmlService.createHtmlOutput(html)
    .setTitle('建設業許可管理システム — ログイン')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

/**
 * OAuthコールバック処理
 * @param {string} code
 * @param {string} state
 * @return {GoogleAppsScript.HTML.HtmlOutput}
 */
function handleOAuthCallback_(code, state) {
  var cache = CacheService.getScriptCache();

  // nonce 取得（state消費前に確認）
  var expectedNonce = cache.get('oauth_state_' + state);
  if (!expectedNonce) {
    // リロード保護: 同一 state で二度目のアクセスは index.html を返す
    var wasDone = cache.get('oauth_done_' + state);
    if (wasDone) {
      return serveIndexWithAuth_('', getAppExecUrl_());
    }
    return errorPage_('認証エラー: リクエストが無効または期限切れです（stateが見つかりません）');
  }

  // state を消費
  cache.remove('oauth_state_' + state);

  // Authorization code → Token exchange
  var clientSecret = PropertiesService.getScriptProperties().getProperty('GOOGLE_CLIENT_SECRET');
  if (!clientSecret) return errorPage_('設定エラー: GOOGLE_CLIENT_SECRET が未設定です');

  var tokenResp = UrlFetchApp.fetch('https://oauth2.googleapis.com/token', {
    method: 'post',
    payload: {
      code: code,
      client_id: getConfig('GOOGLE_CLIENT_ID'),
      client_secret: clientSecret,
      redirect_uri: getAppExecUrl_(),
      grant_type: 'authorization_code'
    },
    muteHttpExceptions: true
  });

  if (tokenResp.getResponseCode() !== 200) {
    return errorPage_('トークン取得に失敗しました (status=' + tokenResp.getResponseCode() + ')');
  }

  var tokens = JSON.parse(tokenResp.getContentText());
  if (!tokens.id_token) return errorPage_('ID Tokenが取得できませんでした');

  // nonce 検証
  try {
    var parts = tokens.id_token.split('.');
    var idPayload = JSON.parse(Utilities.newBlob(Utilities.base64DecodeWebSafe(parts[1])).getDataAsString());
    if (idPayload.nonce !== expectedNonce) {
      logAuthEvent_('NONCE_MISMATCH', 'state=' + state);
      return errorPage_('認証エラー: nonceが一致しません');
    }
  } catch (e) {
    return errorPage_('JWT解析エラー: ' + String(e.message || e));
  }

  // ID Token 検証 + ホワイトリスト確認
  var loginResult = apiLoginWithGoogle(tokens.id_token);
  if (!loginResult || loginResult._error) {
    return errorPage_(loginResult ? loginResult.message : '認証エラー');
  }

  // リロード保護フラグを立てる
  cache.put('oauth_done_' + state, '1', 300);

  var authResultJson = JSON.stringify({
    email: loginResult.email,
    displayName: loginResult.displayName,
    role: loginResult.role
  });

  return serveIndexWithAuth_(authResultJson, getAppExecUrl_());
}

/**
 * index.html を serverAuthResult 付きで返す（Bug1: JSON を \u003c でエスケープ）
 * @param {string} authResultJson
 * @param {string} appExecUrl
 * @return {GoogleAppsScript.HTML.HtmlOutput}
 */
function serveIndexWithAuth_(authResultJson, appExecUrl) {
  var template = HtmlService.createTemplateFromFile('index');
  // Bug1: <?= ?> が < を HTML entity に変換しないよう \u003c に置換
  template.serverAuthResult = (authResultJson || '').replace(/</g, '\\u003c');
  template.appExecUrl = appExecUrl || '';
  return template.evaluate()
    .setTitle('建設業許可管理システム')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

// ---------------------------------------------------------------------------
// Manual Login（管理者用テスト）
// ---------------------------------------------------------------------------

/**
 * 手動ログイン（?manualLogin=email&key=tscg2026）
 * @param {string} email
 * @return {GoogleAppsScript.HTML.HtmlOutput}
 */
function handleManualLogin_(email) {
  email = String(email || '').trim().toLowerCase();
  if (!email) return errorPage_('メールアドレスが指定されていません');

  var access = getUserAccessByEmail_(email);
  if (!access || !access.active) {
    return errorPage_('このメールアドレスはホワイトリストに登録されていません: ' + email);
  }

  logAuthEvent_('MANUAL_LOGIN', 'email=' + email);

  var authResultJson = JSON.stringify({
    email: email,
    displayName: access.displayName || email.split('@')[0],
    role: access.role || 'user'
  });

  return serveIndexWithAuth_(authResultJson, getAppExecUrl_());
}

// ---------------------------------------------------------------------------
// Diagnostic
// ---------------------------------------------------------------------------

/**
 * OAuth診断エンドポイント（?diag=oauth）
 * @return {GoogleAppsScript.Content.TextOutput}
 */
function diagOAuth_() {
  var result = {
    clientId: getConfig('GOOGLE_CLIENT_ID') || '(未設定)',
    appExecUrl: getAppExecUrl_(),
    clientSecretSet: !!PropertiesService.getScriptProperties().getProperty('GOOGLE_CLIENT_SECRET'),
    authMode: getConfig('AUTH_MODE') || '(未設定)',
    timestamp: Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss')
  };
  return ContentService.createTextOutput(JSON.stringify(result, null, 2))
    .setMimeType(ContentService.MimeType.JSON);
}

// ---------------------------------------------------------------------------
// Error Page
// ---------------------------------------------------------------------------

/**
 * エラーページHTMLを返す
 * @param {string} message
 * @return {GoogleAppsScript.HTML.HtmlOutput}
 */
function errorPage_(message) {
  var html =
    '<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">' +
    '<meta name="viewport" content="width=device-width, initial-scale=1">' +
    '<title>エラー — 建設業許可管理システム</title>' +
    '<style>body{font-family:"Segoe UI","Meiryo",sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;background:#f5f5f5;margin:0;}' +
    '.card{background:#fff;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.15);padding:40px 32px;max-width:400px;text-align:center;}' +
    'h1{color:#d93025;font-size:20px;}p{color:#444;font-size:14px;}a{color:#1a73e8;}</style>' +
    '</head><body><div class="card">' +
    '<h1>エラーが発生しました</h1>' +
    '<p>' + (message || '不明なエラーです') + '</p>' +
    '<p><a href="' + getAppExecUrl_() + '?oauthStart=1" target="_top">ログインページに戻る</a></p>' +
    '</div></body></html>';

  return HtmlService.createHtmlOutput(html)
    .setTitle('エラー — 建設業許可管理システム')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

// ---------------------------------------------------------------------------
// UserAccess（ホワイトリスト検索）
// ---------------------------------------------------------------------------

/**
 * メールアドレスで UserAccess を検索（大文字小文字無視）
 * @param {string} email
 * @return {{ email: string, role: string, active: boolean, displayName: string }|null}
 */
function getUserAccessByEmail_(email) {
  var records = readRecords_(SHEETS.UserAccess);
  var target = String(email || '').trim().toLowerCase();
  for (var i = 0; i < records.length; i++) {
    var rowEmail = String(records[i].email || '').trim().toLowerCase();
    if (rowEmail === target) {
      return {
        email: rowEmail,
        role: String(records[i].role || 'user'),
        active: String(records[i].active || '').toLowerCase() === 'true',
        displayName: String(records[i].displayName || '')
      };
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Auth Guards（api.gs から呼び出す）
// ---------------------------------------------------------------------------

/**
 * 認証必須ガード
 * @throws {Error} 未認証の場合
 * @return {{ email: string, role: string, displayName: string }}
 */
function requireAuth_() {
  // Bug2: clientUserKey (OAuth結果) を Session より先にチェック
  if (__authedUser && __authedUser.email) {
    return __authedUser;
  }

  // Fallback: GAS Session（"ユーザーとして実行"デプロイ時）
  var sessionEmail = '';
  try {
    sessionEmail = Session.getActiveUser().getEmail();
  } catch (e) {
    // ignore
  }

  if (sessionEmail) {
    // HYBRID mode: セッションEmailのホワイトリスト確認
    var authMode = getConfig('AUTH_MODE') || 'HYBRID';
    if (authMode === 'OAUTH_REQUIRED') {
      throw new Error('UNAUTHORIZED: OAuth認証が必要です');
    }
    var access = getUserAccessByEmail_(sessionEmail);
    if (access && access.active) {
      return { email: sessionEmail, role: access.role || 'user', displayName: access.displayName || '' };
    }
    // HYBRID fallback: 旧 allowlist チェック
    if (isAuthorized_(sessionEmail)) {
      return { email: sessionEmail, role: 'user', displayName: '' };
    }
  }

  throw new Error('UNAUTHORIZED: 認証されていません');
}

/**
 * 管理者権限必須ガード
 * @throws {Error} 権限不足の場合
 */
function requireAdmin_() {
  var user = requireAuth_();
  if (user.role !== 'admin') {
    throw new Error('FORBIDDEN: 管理者権限が必要です (role=' + user.role + ')');
  }
  return user;
}

// ---------------------------------------------------------------------------
// Auth Event Log
// ---------------------------------------------------------------------------

/**
 * AuthLog シートに認証イベントを記録
 * @param {string} step
 * @param {string} detail
 */
function logAuthEvent_(step, detail) {
  try {
    var sheet = getSheet_(SHEETS.AuthLog);
    var timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
    sheet.appendRow([timestamp, step, detail || '']);
  } catch (e) {
    Logger.log('[AUTH_LOG_ERROR] ' + String(e.message || e));
  }
}
