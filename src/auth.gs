/**
 * auth.gs — Web UIの配信補助
 *
 * 認証判定はSecurity.gsのID token検証だけで行う。GAS Session、effective user、
 * 固定キーによるログインは使用しない。OAuth callbackはCode2.gsから
 * OAuthLogin.gsへ限定的にルーティングする。
 */

function serveSecureIndex_() {
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('建設業許可証管理システム')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.DEFAULT)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1, viewport-fit=cover');
}
