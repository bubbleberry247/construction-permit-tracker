/**
 * Utils.gs — 日付処理・UUID・エラーハンドリング共通ユーティリティ
 */

/**
 * UUID v4 を生成する
 * @return {string}
 */
function generateUuid() {
  return Utilities.getUuid();
}

/**
 * Date を指定フォーマットで文字列化する（Asia/Tokyo固定）
 * @param {Date} date
 * @param {string} fmt  例: 'yyyy/MM/dd'
 * @return {string}
 */
function formatDate(date, fmt) {
  if (!date || !(date instanceof Date) || isNaN(date.getTime())) return '';
  return Utilities.formatDate(date, 'Asia/Tokyo', fmt);
}

/**
 * 日付文字列を Date オブジェクトに変換する
 * @param {string|Date} str  "YYYY/MM/DD" or "YYYY-MM-DD" またはすでに Date
 * @return {Date|null}  無効な場合は null
 */
function parseDate(str) {
  if (!str) return null;
  if (str instanceof Date) {
    return isNaN(str.getTime()) ? null : str;
  }
  var s = String(str).trim().replace(/-/g, '/');
  var m = s.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (!m) return null;
  var d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  if (isNaN(d.getTime())) return null;
  return d;
}

/**
 * 今日から targetDate までの日数を返す（負=過去）
 * @param {Date|string} targetDate
 * @return {number}
 */
function daysUntil(targetDate) {
  var target = parseDate(targetDate);
  if (!target) return NaN;
  var now = new Date();
  // 時刻部分を除いて日付だけで計算
  var todayMs = Date.UTC(now.getFullYear(), now.getMonth(), now.getDate());
  var targetMs = Date.UTC(target.getFullYear(), target.getMonth(), target.getDate());
  return Math.round((targetMs - todayMs) / 86400000);
}

/**
 * ADMIN_EMAILS に対してエラー通知メールを送信する
 * @param {string} subject
 * @param {string} message
 */
function sendErrorAlert(subject, message) {
  var adminEmails = getConfig('ADMIN_EMAILS');
  if (!adminEmails) {
    console.error('ADMIN_EMAILS未設定。エラーアラートを送信できません: ' + subject);
    return;
  }
  var recipients = adminEmails.split(',').map(function(e) { return e.trim(); }).filter(Boolean);
  recipients.forEach(function(email) {
    try {
      GmailApp.sendEmail(email, '[ERROR] ' + subject, message);
    } catch (ex) {
      console.error('エラーアラート送信失敗: ' + ex.message);
    }
  });
}

/**
 * エラーをコンソールと Stackdriver に記録する
 * @param {string} message
 * @param {Error} [error]
 */
function logError(message, error) {
  var detail = message;
  if (error) {
    detail += '\n' + (error.message || String(error));
    if (error.stack) detail += '\n' + error.stack;
  }
  console.error(detail);
}

/**
 * 今日が指定した曜日かどうかを返す（0=日曜, 1=月曜, ...）
 * @param {number} dayOfWeek
 * @return {boolean}
 */
function isTodayDayOfWeek(dayOfWeek) {
  var now = new Date();
  return now.getDay() === dayOfWeek;
}

/**
 * 行政庁名を正規化する（Python側 normalize_authority_name と同等）
 * 「愛知知事」→「愛知県知事」、「国土交通大臣」はそのまま
 * @param {string} rawName  parsePermitNumber_ が返す permit_authority_name
 * @return {string}  正規化済み行政庁名
 */
function normalizeAuthorityName_(rawName) {
  if (!rawName) return '';
  var s = String(rawName).trim();
  // 大臣許可はそのまま
  if (s.indexOf('大臣') !== -1) return '国土交通大臣';
  // 北海道は「県」不要
  if (s.indexOf('北海道') !== -1) return '北海道知事';
  // 都・道・府・県 がすでに付いていれば「知事」を補完して返す
  if (/[都道府県]知事/.test(s)) return s;
  // 「県」が抜けているケース（「愛知知事」→「愛知県知事」）
  // 末尾が「知事」なら間に「県」を挿入
  var m = s.match(/^(.+?)知事$/);
  if (m) {
    var pref = m[1];
    // 東京都・大阪府・京都府・北海道は特殊
    if (pref === '東京' || pref === '東京都') return '東京都知事';
    if (pref === '大阪' || pref === '大阪府') return '大阪府知事';
    if (pref === '京都' || pref === '京都府') return '京都府知事';
    // すでに都道府県が付いていればそのまま
    if (/[都道府県]$/.test(pref)) return pref + '知事';
    // 付いていなければ「県」を補完
    return pref + '県知事';
  }
  // パターン外は素通し
  return s;
}

/**
 * 建設業許可番号文字列をパースして構造化データを返す
 * Python側 permit_parser.py と同等ロジック
 * @param {string} text  例: "愛知県知事 許可（特一 6）第57805号"
 * @return {{permit_authority_name: string, permit_authority_type: string,
 *           permit_category: string, permit_year: number,
 *           contractor_number: string, permit_number_full: string,
 *           parse_success: boolean}}
 */
function parsePermitNumber_(text) {
  var empty = {
    permit_authority_name: '', permit_authority_type: '',
    permit_category: '', permit_year: 0,
    contractor_number: '', permit_number_full: String(text || ''),
    parse_success: false
  };
  if (!text || typeof text !== 'string' || !text.trim()) return empty;

  // 全角→半角 正規化
  var s = text.trim()
    .replace(/（/g, '(').replace(/）/g, ')')
    .replace(/　/g, ' ')
    .replace(/[０-９]/g, function(c) { return String.fromCharCode(c.charCodeAt(0) - 0xFEE0); });

  var pattern = /(.+?)\s*許可\s*\(\s*(特一|般一|特定|一般|特|一|般)[\s\-\u2010\u2012\u2013\u2014\u2015\uFF0D\uFF70]+(\d+)\s*\)\s*第\s*(\d+)\s*号/;
  var m = s.match(pattern);
  if (!m) return empty;

  var rawAuth = m[1].trim();
  var rawCat = m[2].trim();
  var rawYear = m[3];
  var rawNum = m[4];

  var authorityType = rawAuth.indexOf('大臣') !== -1 ? '大臣' : '知事';

  var catMap = { '特一': '特定', '特定': '特定', '特': '特定',
                 '般一': '一般', '一般': '一般', '一': '一般', '般': '一般' };
  var category = catMap[rawCat] || (rawCat.indexOf('特') !== -1 ? '特定' : '一般');

  return {
    permit_authority_name: rawAuth,
    permit_authority_type: authorityType,
    permit_category: category,
    permit_year: parseInt(rawYear, 10) || 0,
    contractor_number: rawNum.trim(),
    permit_number_full: text,
    parse_success: true
  };
}
