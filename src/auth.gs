/**
 * auth.gs — Web UIの配信補助
 *
 * 認証判定はSecurity.gsのID token検証だけで行う。URLパラメータ、GAS Session、
 * effective user、固定キーによるログインは使用しない。
 */

function getAppExecUrl_() {
  try {
    return String(ScriptApp.getService().getUrl() || '');
  } catch (ignored) {
    return '';
  }
}

function serveSecureIndex_() {
  var template = HtmlService.createTemplateFromFile('index');
  template.googleClientId = String(getSecureSetting_('GOOGLE_CLIENT_ID') || '');
  template.appExecUrl = getAppExecUrl_();
  return template.evaluate()
    .setTitle('建設業許可証管理システム')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.DEFAULT)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1, viewport-fit=cover');
}
