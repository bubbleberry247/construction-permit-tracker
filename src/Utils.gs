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
  var m = s.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})(?:[ T].*)?$/);
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
 * ENABLE_SEND が明示的な TRUE の場合だけ送信を許可する。
 * 空欄、未設定、不正値、1、FALSE はすべて false。
 * @return {boolean}
 */
function isSendEnabled_() {
  return String(getConfig('ENABLE_SEND') || '').trim().toUpperCase() === 'TRUE';
}

/**
 * NOTIFY_STAGES_DAYS を厳密に検証して降順の数値配列を返す。
 * 重複値は除去する。空欄・不正形式・0〜365外の値は拒否する。
 * @param {*} rawValue
 * @return {number[]}
 */
function parseNotifyStages_(rawValue) {
  var raw = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
  if (!/^\d{1,3}(,\d{1,3})*$/.test(raw)) {
    var formatError = new Error('NOTIFY_STAGES_DAYSの形式が不正です: ' + raw);
    formatError.code = 'INVALID_NOTIFY_STAGES';
    throw formatError;
  }

  var seen = {};
  var stages = [];
  raw.split(',').forEach(function(part) {
    var value = Number(part);
    if (!Number.isInteger(value) || value < 0 || value > 365) {
      var rangeError = new Error('NOTIFY_STAGES_DAYSは0〜365で指定してください: ' + part);
      rangeError.code = 'INVALID_NOTIFY_STAGES';
      throw rangeError;
    }
    if (!seen[value]) {
      seen[value] = true;
      stages.push(value);
    }
  });

  stages.sort(function(a, b) { return b - a; });
  return stages;
}

/**
 * Config上の日次送信上限を厳密に取得する。
 * 未設定・不正値は安全側に倒して例外とする。
 * @return {number}
 */
function getConfiguredDailySendLimit_() {
  var raw = String(getConfig('GMAIL_DAILY_LIMIT') || '').trim();
  if (!/^\d+$/.test(raw)) {
    throw new Error('GMAIL_DAILY_LIMITが未設定または不正です');
  }
  var limit = Number(raw);
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error('GMAIL_DAILY_LIMITは1以上の整数で指定してください');
  }
  return limit;
}

/**
 * カンマ区切りの宛先を正規化する。
 * @param {*} value
 * @return {string[]}
 */
function normalizeEmailRecipients_(value) {
  return String(value || '').split(',').map(function(email) {
    return email.trim();
  }).filter(Boolean);
}

/**
 * To/CC/BCCを合算した一意な受信者数を返す。
 * @param {string} to
 * @param {Object} options
 * @return {number}
 */
function countEmailRecipients_(to, options) {
  var unique = {};
  normalizeEmailRecipients_(to).concat(
    normalizeEmailRecipients_(options && options.cc),
    normalizeEmailRecipients_(options && options.bcc)
  ).forEach(function(email) {
    unique[email.toLowerCase()] = true;
  });
  return Object.keys(unique).length;
}

/**
 * 送信を行わなかった理由をNotificationsへ記録する。
 * 記録できない場合も実行ログへ残し、メール送信へ進ませない。
 * @param {Object} notificationData
 * @param {string} result
 * @param {string} message
 * @return {Object}
 */
function recordBlockedNotification_(notificationData, result, message) {
  notificationData.result = result;
  notificationData.error_message = message || '';
  var logFailure = '';
  try {
    NotificationsModel.create(notificationData);
  } catch (logErr) {
    logFailure = logErr.message || String(logErr);
    console.error('BLOCKED通知の記録に失敗しました（メールは送信しません）: ' + logFailure);
  }
  console.warn(result + ': ' + (message || 'メール送信を停止しました'));
  return {
    success: false,
    sent: false,
    blocked: true,
    result: result,
    message: message || '',
    logFailure: logFailure
  };
}

/**
 * システム内の全メール送信を通す共通ゲート。
 * ENABLE_SEND、設定上限、Gmail実残量、intent logを順に確認する。
 * @param {{
 *   to: string,
 *   subject: string,
 *   body: string,
 *   options: Object,
 *   notification: Object
 * }} params
 * @return {Object}
 */
function sendSystemEmail_(params) {
  params = params || {};
  var to = String(params.to || '').trim();
  var subject = String(params.subject || '');
  var body = String(params.body || '');
  var options = params.options || {};
  var notificationData = params.notification || {};

  notificationData.to_email = notificationData.to_email || to;
  notificationData.cc_email = notificationData.cc_email || String(options.cc || '');
  notificationData.subject = notificationData.subject || subject;
  notificationData.body = notificationData.body || body;
  notificationData.stage = notificationData.stage || 'SYSTEM';
  notificationData.result = '';
  notificationData.error_message = '';

  if (!isSendEnabled_()) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_SEND_DISABLED',
      'ENABLE_SENDが明示的なTRUEではありません'
    );
  }

  var recipientCount = countEmailRecipients_(to, options);
  if (!to || recipientCount <= 0) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_RECIPIENT',
      '送信先が未設定です'
    );
  }

  var configuredLimit;
  try {
    configuredLimit = getConfiguredDailySendLimit_();
  } catch (configErr) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_CONFIG',
      configErr.message || String(configErr)
    );
  }

  // PENDINGを予約済みとして数え、同一実行ループ中の上限超過を抑止する。
  var reservedOrSent;
  try {
    reservedOrSent = NotificationsModel.countReservedOrSentToday();
  } catch (countErr) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_LOG_FAILURE',
      '送信済み件数を確認できません: ' + (countErr.message || String(countErr))
    );
  }
  if (reservedOrSent >= configuredLimit) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_CONFIG_LIMIT',
      '設定上の日次送信上限に達しました: ' + configuredLimit
    );
  }

  var providerRemaining;
  try {
    providerRemaining = Number(MailApp.getRemainingDailyQuota());
  } catch (quotaErr) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_QUOTA_CHECK',
      'Gmail実残量を確認できません: ' + (quotaErr.message || String(quotaErr))
    );
  }
  if (!Number.isFinite(providerRemaining) || providerRemaining < recipientCount) {
    return recordBlockedNotification_(
      notificationData,
      'BLOCKED_PROVIDER_QUOTA',
      'Gmail実残量が不足しています: remaining=' + providerRemaining +
        ', required=' + recipientCount
    );
  }

  // 送信前にintentを記録する。記録失敗時は送信しない。
  notificationData.result = 'PENDING';
  var created;
  try {
    created = NotificationsModel.create(notificationData);
  } catch (intentErr) {
    console.error(
      'PENDING記録に失敗したためメールを送信しません: ' +
        (intentErr.message || String(intentErr))
    );
    return {
      success: false,
      sent: false,
      blocked: true,
      result: 'BLOCKED_LOG_FAILURE',
      message: intentErr.message || String(intentErr)
    };
  }
  var notificationId = created.notification_id;

  try {
    GmailApp.sendEmail(to, subject, body, options);
  } catch (sendErr) {
    try {
      NotificationsModel.updateById(notificationId, {
        result: 'FAILED',
        error_message: sendErr.message || String(sendErr)
      });
    } catch (failedLogErr) {
      console.error(
        '送信失敗後のNotifications更新にも失敗しました: ' +
          (failedLogErr.message || String(failedLogErr))
      );
    }
    logError('メール送信エラー', sendErr);
    return {
      success: false,
      sent: false,
      blocked: false,
      result: 'FAILED',
      message: sendErr.message || String(sendErr),
      notificationId: notificationId
    };
  }

  // ここへ到達した時点でGmail送信は成功済み。ログ更新失敗と混同しない。
  var sentLogUpdated = false;
  var sentLogError = '';
  try {
    sentLogUpdated = NotificationsModel.updateById(
      notificationId,
      { result: 'SENT', error_message: '' }
    );
    if (!sentLogUpdated) {
      sentLogError = 'notification_idが見つかりません';
    }
  } catch (sentLogErr) {
    sentLogError = sentLogErr.message || String(sentLogErr);
  }
  if (!sentLogUpdated) {
    console.error(
      'メール送信は成功しましたがNotifications更新に失敗しました: ' +
        notificationId + ' ' + sentLogError
    );
  }

  return {
    success: true,
    sent: true,
    blocked: false,
    result: 'SENT',
    notificationId: notificationId,
    logUpdated: sentLogUpdated,
    logError: sentLogError
  };
}

/**
 * ADMIN_EMAILS に対して内部エラー通知メールを送信する。
 * 末尾 "_" の非公開関数とし、クライアント入力から直接起動できない。
 * @param {string} subject
 * @param {string} message
 * @return {Object}
 */
function sendErrorAlert_(subject, message) {
  var recipients = normalizeEmailRecipients_(getConfig('ADMIN_EMAILS'));
  if (recipients.length === 0) {
    console.error('ADMIN_EMAILS未設定。エラーアラートを送信できません: ' + subject);
    return {
      success: false,
      sent: false,
      blocked: true,
      result: 'BLOCKED_RECIPIENT'
    };
  }

  try {
    return sendSystemEmail_({
      to: recipients[0],
      subject: '[ERROR] ' + String(subject || ''),
      body: String(message || ''),
      options: recipients.length > 1 ? { bcc: recipients.slice(1).join(',') } : {},
      notification: {
        company_id: '',
        permit_id: '',
        to_email: recipients[0],
        cc_email: '',
        stage: 'ERROR_ALERT',
        subject: '[ERROR] ' + String(subject || ''),
        body: String(message || '')
      }
    });
  } catch (alertErr) {
    console.error(
      'エラーアラート処理失敗（メールは送信されていない可能性があります）: ' +
        (alertErr.message || String(alertErr))
    );
    return {
      success: false,
      sent: false,
      blocked: true,
      result: 'BLOCKED_INTERNAL_ERROR',
      message: alertErr.message || String(alertErr)
    };
  }
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
