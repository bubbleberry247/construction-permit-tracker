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
      '90':  '許可証の有効期限まで約90日です。更新手続きの予定をご確認ください。',
      '60':  '許可証の有効期限まで約60日です。更新手続きの状況をご確認ください。',
      '30':  '【要確認】許可証の有効期限まで約30日です。更新状況をご確認ください。',
      'EXPIRED': '許可証の有効期限が過ぎています。更新済みの場合は新しい許可情報をご連絡ください。未更新の場合は、発注・入場に影響が生じる可能性がありますのでご確認ください。'
    };
    return messages[String(stage)] || '許可証の更新状況をご確認ください。';
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
    var expiryDateStr = formatDate_(
      permit.expiry_date instanceof Date ? permit.expiry_date : parseDate_(permit.expiry_date),
      'yyyy/MM/dd'
    );
    var subject = '【重要】建設業許可 更新手続のお願い（満了日：' + expiryDateStr + '）';
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
      '■ 更新完了時のご連絡\n' +
      '更新後の許可情報は、以下のお問い合わせ先へご連絡ください。\n\n' +
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

    body += '\n\n■ ご対応のお願い\n期限差分がある場合は、システムのMLIT確認画面で公表情報と承認済み期限を確認してください。';

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
