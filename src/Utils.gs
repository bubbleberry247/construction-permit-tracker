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
