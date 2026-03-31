/**
 * Code2.gs — Webapp エントリポイント（doGet）
 *
 * 既存の Code.gs がないため Code2.gs として追加。
 * Webアプリのメインエントリポイント。
 */

// ---------------------------------------------------------------------------
// Allowed emails（Auth allowlist）
// ---------------------------------------------------------------------------
function getAllowedEmails_() {
  var config = loadConfigAll_();
  var allowed = config['ALLOWED_EMAILS'] || config['ADMIN_EMAILS'] || '';
  return allowed.split(',').map(function(e) { return e.trim().toLowerCase(); }).filter(Boolean);
}

function isAuthorized_(email) {
  if (!email) return false;
  var allowed = getAllowedEmails_();
  if (allowed.length === 0) return true; // allowlist未設定時は全員許可（fail-open注意）
  return allowed.indexOf(email.toLowerCase()) >= 0;
}

/**
 * 現在のユーザーメールを取得（デプロイ設定に応じて適切な方法で）
 * 「ウェブアプリにアクセスしているユーザーとして実行」の場合: getEffectiveUser()
 * 「自分として実行」の場合: getActiveUser()（空になることがある）
 */
function getCurrentUserEmail_() {
  // Try activeUser first (works when deployed as "User accessing the web app")
  var email = Session.getActiveUser().getEmail();
  if (email) return email;
  // Fallback to effectiveUser (always returns the script owner when "Execute as me")
  email = Session.getEffectiveUser().getEmail();
  return email || 'unknown';
}

// ---------------------------------------------------------------------------
// doGet — Webアプリエントリポイント（OAuth 2.0ルーティング）
// ---------------------------------------------------------------------------
function doGet(e) {
  var params = (e && e.parameter) ? e.parameter : {};
  var diag    = params.diag    || '';
  var error   = params.error   || '';
  var code    = params.code    || '';
  var state   = params.state   || '';
  var action  = params.action  || '';
  var oauthStart  = params.oauthStart  || '';
  var manualLogin = params.manualLogin || '';
  var manualKey   = params.key || '';

  // 1. 診断エンドポイント
  if (diag === 'oauth') {
    return diagOAuth_();
  }

  // 2. エラーパラメータ
  if (error) {
    return errorPage_('OAuth エラー: ' + error);
  }

  // 3. OAuthコールバック（code + state）
  if (code && state) {
    return handleOAuthCallback_(code, state);
  }

  // 4. OAuthログイン開始ページ
  if (oauthStart === '1') {
    return generateOAuthStartPage_();
  }

  // 5. 管理者手動ログイン（?manualLogin=email&key=tscg2026）
  if (manualLogin && manualKey === 'tscg2026') {
    return handleManualLogin_(manualLogin);
  }

  // 6. Admin endpoints（後方互換）
  if (action === 'initAuditLog') {
    getSheet_(SHEETS.AuditLog);
    return ContentService.createTextOutput(JSON.stringify({ ok: true }))
      .setMimeType(ContentService.MimeType.JSON);
  }
  if (action === 'initUserAccess') {
    getSheet_(SHEETS.UserAccess);
    getSheet_(SHEETS.AuthLog);
    return ContentService.createTextOutput(JSON.stringify({ ok: true, sheets: ['UserAccess', 'AuthLog'] }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  // 7. Default: AUTH_MODE に応じてサーブ
  var authMode = getConfig('AUTH_MODE') || 'HYBRID';
  var hasOAuthConfig = !!getConfig('GOOGLE_CLIENT_ID');

  if (authMode === 'OAUTH_REQUIRED') {
    if (!hasOAuthConfig) {
      return errorPage_('設定エラー: OAUTH_REQUIRED ですが GOOGLE_CLIENT_ID が未設定です');
    }
    return generateOAuthStartPage_();
  }

  // HYBRID モード: OAuth未設定時は旧方式のみで動作（後方互換）
  var email = getCurrentUserEmail_();
  if (isAuthorized_(email) && email && email !== 'unknown') {
    // 旧 allowlist で認証OK → GASセッション情報をserverAuthResultとして渡す
    var sessionAuth = JSON.stringify({
      email: email,
      displayName: email.split('@')[0],
      role: 'user'
    });
    return serveIndexWithAuth_(sessionAuth, getAppExecUrl_());
  }

  // 旧認証NG → OAuth が設定されていればログインページへ
  if (hasOAuthConfig) {
    return generateOAuthStartPage_();
  }

  // OAuth も未設定、allowlist未設定 = fail-open → セッション情報を渡す
  var fallbackEmail = (email && email !== 'unknown') ? email : '';
  if (fallbackEmail) {
    var fallbackAuth = JSON.stringify({
      email: fallbackEmail,
      displayName: fallbackEmail.split('@')[0],
      role: 'user'
    });
    return serveIndexWithAuth_(fallbackAuth, getAppExecUrl_());
  }

  // 完全に未認証 → ログイン画面（OAuth未設定でもログインページは表示）
  return serveIndexWithAuth_('', getAppExecUrl_());
}
