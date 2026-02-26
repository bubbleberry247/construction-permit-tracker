/**
 * Config.gs — Configシートからキー/バリューで設定値を読み込む
 */

var CONFIG_CACHE_ = null;

/**
 * Configシートの全データをキャッシュして返す
 * @return {Object} key → value のオブジェクト
 */
function loadConfigAll_() {
  if (CONFIG_CACHE_) return CONFIG_CACHE_;

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('Config');
  if (!sheet) throw new Error('Configシートが見つかりません');

  var data = sheet.getDataRange().getValues();
  var config = {};
  for (var i = 1; i < data.length; i++) {
    var key = String(data[i][0]).trim();
    var value = String(data[i][1]).trim();
    if (key) config[key] = value;
  }
  CONFIG_CACHE_ = config;
  return config;
}

/**
 * 指定キーの設定値を返す
 * @param {string} key
 * @return {string} 値（未設定時は空文字）
 */
function getConfig(key) {
  var config = loadConfigAll_();
  return config[key] !== undefined ? config[key] : '';
}

/**
 * 必須キーが全て設定されているか検証する
 * @return {string[]} 未設定のキー一覧（全て揃っていれば空配列）
 */
function checkConfig() {
  var REQUIRED_KEYS = [
    'ADMIN_EMAILS',
    'DRIVE_ROOT_FOLDER_ID',
    'FORM_ID',
    'NOTIFY_STAGES_DAYS',
    'RUN_TIMEZONE',
    'ENABLE_SEND'
  ];
  var config = loadConfigAll_();
  var missing = [];
  REQUIRED_KEYS.forEach(function(key) {
    if (!config[key]) missing.push(key);
  });
  return missing;
}

/**
 * キャッシュをクリアする（テスト・再読み込み用）
 */
function clearConfigCache() {
  CONFIG_CACHE_ = null;
}
