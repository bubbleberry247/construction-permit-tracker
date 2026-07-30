/**
 * Code2.gs — Webアプリ唯一のHTTPエントリポイント
 */

function doGet(event) {
  if (isOAuthCallbackRequest_(event)) {
    return handleOAuthCallback_(event);
  }
  return serveSecureIndex_();
}
