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
// doGet — Webアプリエントリポイント
// ---------------------------------------------------------------------------
function doGet(e) {
  var email = getCurrentUserEmail_();

  // Auth check
  if (!isAuthorized_(email)) {
    return HtmlService.createHtmlOutput(
      '<html><body style="font-family:Meiryo,sans-serif;text-align:center;padding:60px;">' +
      '<h1>アクセス権限がありません</h1>' +
      '<p>管理者にお問い合わせください。</p>' +
      '<p style="color:#999;">Email: ' + email + '</p>' +
      '</body></html>'
    ).setTitle('アクセス拒否');
  }

  // Admin endpoints
  var action = (e && e.parameter && e.parameter.action) ? e.parameter.action : '';

  if (action === 'initAuditLog') {
    getSheet_(SHEETS.AuditLog);
    return ContentService.createTextOutput(JSON.stringify({ok: true}))
      .setMimeType(ContentService.MimeType.JSON);
  }

  // Default: serve SPA
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('建設業許可管理システム')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}
