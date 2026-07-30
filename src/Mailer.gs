/**
 * Mailer.gs — メールテンプレートと送信
 */

var Mailer = {

  /**
   * ステージ別の通知文言を返す
   * @param {string} stage
   * @return {string}
   */
  _getStageMessage: function(stage) {
    var messages = {
      '90':  '許可証の有効期限まで約90日です。更新が完了しましたら、新しい許可証PDFをGoogleフォーム経由でご提出ください。',
      '60':  '許可証の有効期限まで約60日です。更新手続きはお済みでしょうか。完了後は新しい許可証PDFをGoogleフォーム経由でご提出ください。',
      '30':  '【要確認】許可証の有効期限まで約30日です。更新状況をご確認ください。更新済みの場合は、新しい許可証PDFをGoogleフォーム経由でご提出ください。',
      'EXPIRED': '許可証の有効期限が過ぎています。更新済みの場合は、新しい許可証PDFを至急ご提出ください。未更新の場合は、発注・入場に影響が生じる可能性がありますのでご確認ください。'
    };
    return messages[String(stage)] || '許可証の更新状況をご確認ください。更新済みの場合は、新しい許可証PDFをGoogleフォーム経由でご提出ください。';
  },

  /**
   * 期限通知の宛先・件名・本文を生成する。ここでは送信しない。
   * @param {Object} permit
   * @param {Object} company
   * @param {string} stage
   * @return {Object}
   */
  buildExpiryNotification: function(permit, company, stage) {
    var adminEmails = getConfig_('ADMIN_EMAILS');
    var formId = getConfig_('FORM_ID');
    var expiryDateStr = formatDate_(
      permit.expiry_date instanceof Date ? permit.expiry_date : parseDate_(permit.expiry_date),
      'yyyy/MM/dd'
    );
    var subject = '【重要】建設業許可 更新手続のお願い（満了日：' + expiryDateStr + '）';
    var formUrl = formId
      ? 'https://docs.google.com/forms/d/' + formId + '/viewform'
      : '（フォームURL未設定）';
    var contactInfo = getConfig_('CONTACT_INFO') || '本メールの送信元までご連絡ください';
    var stageMessage = this._getStageMessage(stage);
    var companyName = company.company_name_normalized || company.company_name_raw || '';
    var body =
      companyName + ' ' + (company.contact_person || '') + ' 様\n\n' +
      'いつもお世話になっております。\n' +
      '建設業許可証の更新に関してご連絡いたします。\n\n' +
      '■ 許可証情報\n' +
      '　会社名：' + companyName + '\n' +
      '　許可番号：' + permit.permit_number_full + '\n' +
      '　満了日：' + expiryDateStr + '\n\n' +
      '■ ご連絡内容\n' +
      stageMessage + '\n\n' +
      '■ 許可証・受付票の提出はこちら\n' +
      formUrl + '\n\n' +
      '■ お問い合わせ先\n' +
      contactInfo + '\n\n' +
      '何卒よろしくお願いいたします。';

    var ccList = normalizeEmailRecipients_(company.contact_email_cc);
    var bccList = [];
    var stageNum = parseInt(String(stage), 10);
    var isHighAlert = stage === 'EXPIRED' || (!isNaN(stageNum) && stageNum <= 30);
    if (isHighAlert) bccList = normalizeEmailRecipients_(adminEmails);

    return {
      company_id: String(company.company_id || ''),
      permit_id: String(permit.permit_id || ''),
      to_email: normalizeEmailAddress_(company.contact_email),
      cc_email: ccList.join(','),
      bcc_email: bccList.join(','),
      stage: String(stage),
      subject: subject,
      body: body
    };
  },

  /**
   * 期限通知メールを送信する
   * @param {Object} permit
   * @param {Object} company
   * @param {string} stage
   */
  sendExpiryNotification: function(permit, company, stage) {
    var built = this.buildExpiryNotification(permit, company, stage);
    var mailOptions = {};
    if (built.cc_email) mailOptions.cc = built.cc_email;
    if (built.bcc_email) mailOptions.bcc = built.bcc_email;
    return sendSystemEmail_({
      to: built.to_email,
      subject: built.subject,
      body: built.body,
      options: mailOptions,
      notification: Object.assign({}, built, { result: '', error_message: '' })
    });
  },

  /**
   * 許可証受領確認メールを送信する
   * @param {Object} permit
   * @param {Object} company
   */
  sendReceiptConfirmation: function(permit, company) {
    var adminEmails = getConfig_('ADMIN_EMAILS');

    var expiryDateStr = formatDate_(
      permit.expiry_date instanceof Date ? permit.expiry_date : parseDate_(permit.expiry_date),
      'yyyy/MM/dd'
    );

    var subject = '【受領確認】建設業許可証を受領しました（' + (company.company_name_normalized || company.company_name_raw) + '）';

    // 次回通知予定ステージを算出
    var stageDays;
    try {
      stageDays = parseNotifyStages_(getConfig_('NOTIFY_STAGES_DAYS'));
    } catch (stageErr) {
      logError_('受領確認メール停止: NOTIFY_STAGES_DAYS不正', stageErr);
      return recordBlockedNotification_({
        company_id: company.company_id,
        permit_id: permit.permit_id,
        to_email: company.contact_email,
        cc_email: '',
        stage: 'RECEIPT',
        subject: subject,
        body: ''
      }, 'BLOCKED_INVALID_STAGES', stageErr.message || String(stageErr));
    }
    var days = daysUntil_(permit.expiry_date);
    var nextStage = '（算出不可）';
    for (var i = 0; i < stageDays.length; i++) {
      if (days > stageDays[i]) {
        nextStage = '満了' + stageDays[i] + '日前（約 ' +
          formatDate_(new Date(new Date().getTime() + (days - stageDays[i]) * 86400000), 'yyyy/MM/dd') + '）';
        break;
      }
    }

    var body =
      (company.company_name_normalized || company.company_name_raw) + ' ' + (company.contact_person || '') + ' 様\n\n' +
      'この度は建設業許可証をご提出いただきありがとうございます。\n' +
      '以下の内容で受領いたしましたのでご確認ください。\n\n' +
      '■ 受領内容\n' +
      '　許可番号：' + permit.permit_number_full + '\n' +
      '　満了日：' + expiryDateStr + '\n\n' +
      '■ 次回通知予定\n' +
      '　' + nextStage + '\n\n' +
      'ご不明な点がございましたらご連絡ください。\n' +
      'よろしくお願いいたします。';

    var notificationData = {
      company_id: company.company_id,
      permit_id:  permit.permit_id,
      to_email:   company.contact_email,
      cc_email:   '',
      bcc_email:  adminEmails || '',
      stage:      'RECEIPT',
      subject:    subject,
      body:       body,
      result:     '',
      error_message: ''
    };

    var mailOptions = {};
    if (adminEmails) mailOptions.bcc = adminEmails;

    return sendSystemEmail_({
      to: company.contact_email,
      subject: subject,
      body: body,
      options: mailOptions,
      notification: notificationData
    });
  },

  /**
   * テスト送信
   * @param {string} toEmail
   */
  sendTestEmail: function(toEmail) {
    var subject = '【テスト】建設業許可証管理システム テスト送信';
    var body =
      'このメールは建設業許可証管理システムのテスト送信です。\n\n' +
      '送信日時: ' + formatDate_(new Date(), 'yyyy/MM/dd HH:mm:ss') + '\n\n' +
      '正常に受信できていれば、メール送信設定は正しく動作しています。';

    return sendSystemEmail_({
      to: toEmail,
      subject: subject,
      body: body,
      options: {},
      sendOrigin: 'SYSTEM_INTERNAL',
      notification: {
        company_id: '',
        permit_id: '',
        to_email: toEmail,
        cc_email: '',
        stage: 'TEST',
        subject: subject,
        body: body
      }
    });
  },

  /**
   * 月次サマリーメールを ADMIN_EMAILS に送信する
   */
  sendMonthlySummary: function() {
    var adminEmails = getConfig_('ADMIN_EMAILS');
    if (!adminEmails) return;

    var permits = PermitsModel.getAllActive();
    var today = new Date();

    // 90日以内の許可証を抽出
    var nearExpiry = permits.filter(function(p) {
      var d = daysUntil_(p.expiry_date);
      return !isNaN(d) && d <= 90;
    }).sort(function(a, b) {
      return daysUntil_(a.expiry_date) - daysUntil_(b.expiry_date);
    });

    var subject = '【月次レポート】建設業許可 期限接近一覧';

    var lines = [
      '■ 期限90日以内の許可証一覧',
      '集計日: ' + formatDate_(today, 'yyyy/MM/dd'),
      '件数: ' + nearExpiry.length + '件',
      '',
      ['会社名', '許可番号', '満了日', '残日数', 'ステータス'].join('\t')
    ];

    nearExpiry.forEach(function(p) {
      var company = CompaniesModel.findById(p.company_id);
      var companyName = company ? (company.company_name_normalized || company.company_name_raw) : p.company_id;
      var expiryStr = formatDate_(
        p.expiry_date instanceof Date ? p.expiry_date : parseDate_(p.expiry_date),
        'yyyy/MM/dd'
      );
      var d = daysUntil_(p.expiry_date);
      var daysStr = isNaN(d) ? '不明' : (d < 0 ? '期限切れ(' + Math.abs(d) + '日経過)' : d + '日');
      lines.push([companyName, p.permit_number_full, expiryStr, daysStr, p.current_status || ''].join('\t'));
    });

    var body = lines.join('\n');

    body += '\n\n■ ご対応のお願い\n更新が完了した業者様には、新しい許可証PDFをGoogleフォーム経由でご提出いただくようご案内ください。';

    var recipients = normalizeEmailRecipients_(adminEmails);
    if (recipients.length === 0) return;
    var mailOptions = {};
    if (recipients.length > 1) mailOptions.cc = recipients.slice(1).join(',');
    // permit_id列を非permit通知の冪等キーにも利用する。
    // 同じ月のPENDING/SENTがあれば中央送信ゲートが自動再送を止める。
    var monthlyIdempotencyKey = 'MONTHLY:' + formatDate_(today, 'yyyy-MM');

    return sendSystemEmail_({
      to: recipients[0],
      subject: subject,
      body: body,
      options: mailOptions,
      notification: {
        company_id: '',
        permit_id: monthlyIdempotencyKey,
        to_email: recipients[0],
        cc_email: mailOptions.cc || '',
        stage: 'MONTHLY',
        subject: subject,
        body: body
      }
    });
  }
};
