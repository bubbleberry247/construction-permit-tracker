/**
 * Mailer.gs — メールテンプレートと送信
 */

var Mailer = {

  /**
   * Gmail 送信上限チェック（1日150件で停止）
   * @return {boolean}  true = 送信可能
   */
  _checkDailyLimit: function() {
    var sent = NotificationsModel.countSentToday();
    if (sent >= 150) {
      sendErrorAlert(
        'Gmail日次送信上限に達しました',
        '本日の送信件数が150件を超えました。残りの通知はスキップされます。'
      );
      return false;
    }
    return true;
  },

  /**
   * ステージ別の通知文言を返す
   * @param {string} stage
   * @return {string}
   */
  _getStageMessage: function(stage) {
    var messages = {
      '120': '許可証の更新手続の準備を開始してください。満了日まで120日前後となりました。',
      '90':  '申請手続を開始してください。また、受付票（控え）の提出もお願いします。満了日まで90日前後となりました。',
      '60':  '更新手続の進捗をご確認ください。受付票（控え）をまだご提出いただいていない場合は、至急ご提出ください。満了日まで60日前後となりました。',
      '45':  '受付票（控え）が未提出の場合は至急ご提出ください。満了日まで45日前後となりました。',
      '30':  '【最終警告】受付票が未提出の場合、発注・入場に影響が生じる可能性があります。満了日まで30日前後となりました。速やかにご対応ください。',
      '14':  '【発注停止予告】許可証の有効期限まで14日です。至急状況をご確認の上、ご連絡ください。',
      '0':   '本日が許可証の満了日です。更新状況をご確認ください。',
      'EXPIRED': '許可証の有効期限が切れています。至急更新手続きをご確認ください。このまま更新が確認できない場合、発注・入場を停止させていただく場合があります。'
    };
    return messages[String(stage)] || '許可証の更新についてご確認ください。';
  },

  /**
   * 期限通知メールを送信する
   * @param {Object} permit
   * @param {Object} company
   * @param {string} stage
   */
  sendExpiryNotification: function(permit, company, stage) {
    var enableSend = getConfig('ENABLE_SEND');
    var adminEmails = getConfig('ADMIN_EMAILS');
    var formId = getConfig('FORM_ID');

    var expiryDateStr = formatDate(
      permit.expiry_date instanceof Date ? permit.expiry_date : parseDate(permit.expiry_date),
      'yyyy/MM/dd'
    );

    var subject = '【重要】建設業許可 更新手続のお願い（満了日：' + expiryDateStr + '）';

    var formUrl = formId ? 'https://docs.google.com/forms/d/' + formId + '/viewform' : '（フォームURL未設定）';
    var adminEmailList = adminEmails ? adminEmails.split(',').map(function(e) { return e.trim(); }).join('、') : '（未設定）';

    var stageMessage = this._getStageMessage(stage);

    var body =
      company.company_name + ' ' + (company.contact_person || '') + ' 様\n\n' +
      'いつもお世話になっております。\n' +
      '建設業許可証の更新に関してご連絡いたします。\n\n' +
      '■ 許可証情報\n' +
      '　会社名：' + company.company_name + '\n' +
      '　許可番号：' + permit.permit_number + '\n' +
      '　満了日：' + expiryDateStr + '\n\n' +
      '■ ご連絡内容\n' +
      stageMessage + '\n\n' +
      '■ 許可証・受付票の提出はこちら\n' +
      formUrl + '\n\n' +
      '■ お問い合わせ先\n' +
      adminEmailList + '\n\n' +
      '何卒よろしくお願いいたします。';

    // CC の組み立て
    var ccList = [];
    if (company.contact_email_cc) ccList.push(company.contact_email_cc);

    // stage が '30' 以下または 'EXPIRED' の場合は ADMIN_EMAILS もCC
    var stageNum = parseInt(String(stage), 10);
    var isHighAlert = stage === 'EXPIRED' || (!isNaN(stageNum) && stageNum <= 30);
    if (isHighAlert && adminEmails) {
      adminEmails.split(',').forEach(function(e) {
        var trimmed = e.trim();
        if (trimmed) ccList.push(trimmed);
      });
    }

    var notificationData = {
      company_id: company.company_id,
      permit_id:  permit.permit_id,
      to_email:   company.contact_email,
      cc_email:   ccList.join(','),
      stage:      String(stage),
      subject:    subject,
      body:       body,
      result:     '',
      error_message: ''
    };

    // ENABLE_SEND='false' の場合はドライラン
    if (enableSend === 'false') {
      notificationData.result = 'DRY_RUN';
      NotificationsModel.create(notificationData);
      return;
    }

    // 送信上限チェック
    if (!this._checkDailyLimit()) {
      notificationData.result = 'SKIPPED_LIMIT';
      NotificationsModel.create(notificationData);
      return;
    }

    try {
      var mailOptions = { subject: subject };
      if (ccList.length > 0) mailOptions.cc = ccList.join(',');

      GmailApp.sendEmail(company.contact_email, subject, body, mailOptions);
      notificationData.result = 'SENT';
    } catch (err) {
      logError('sendExpiryNotification 送信エラー', err);
      notificationData.result = 'FAILED';
      notificationData.error_message = err.message || String(err);
    }

    NotificationsModel.create(notificationData);
  },

  /**
   * 許可証受領確認メールを送信する
   * @param {Object} permit
   * @param {Object} company
   */
  sendReceiptConfirmation: function(permit, company) {
    var enableSend = getConfig('ENABLE_SEND');
    var adminEmails = getConfig('ADMIN_EMAILS');

    var expiryDateStr = formatDate(
      permit.expiry_date instanceof Date ? permit.expiry_date : parseDate(permit.expiry_date),
      'yyyy/MM/dd'
    );

    var subject = '【受領確認】建設業許可証を受領しました（' + company.company_name + '）';

    // 次回通知予定ステージを算出
    var stagesStr = getConfig('NOTIFY_STAGES_DAYS') || '120,90,60,45,30,14,0';
    var stageDays = stagesStr.split(',').map(function(s) { return parseInt(s.trim(), 10); })
                             .filter(function(n) { return !isNaN(n); })
                             .sort(function(a, b) { return b - a; });
    var days = daysUntil(permit.expiry_date);
    var nextStage = '（算出不可）';
    for (var i = 0; i < stageDays.length; i++) {
      if (days > stageDays[i]) {
        nextStage = '満了' + stageDays[i] + '日前（約 ' +
          formatDate(new Date(new Date().getTime() + (days - stageDays[i]) * 86400000), 'yyyy/MM/dd') + '）';
        break;
      }
    }

    var body =
      company.company_name + ' ' + (company.contact_person || '') + ' 様\n\n' +
      'この度は建設業許可証をご提出いただきありがとうございます。\n' +
      '以下の内容で受領いたしましたのでご確認ください。\n\n' +
      '■ 受領内容\n' +
      '　許可番号：' + permit.permit_number + '\n' +
      '　満了日：' + expiryDateStr + '\n\n' +
      '■ 次回通知予定\n' +
      '　' + nextStage + '\n\n' +
      'ご不明な点がございましたらご連絡ください。\n' +
      'よろしくお願いいたします。';

    if (enableSend === 'false') return;

    try {
      var mailOptions = { subject: subject };
      if (adminEmails) mailOptions.bcc = adminEmails;

      GmailApp.sendEmail(company.contact_email, subject, body, mailOptions);
    } catch (err) {
      logError('sendReceiptConfirmation 送信エラー', err);
    }
  },

  /**
   * テスト送信
   * @param {string} toEmail
   */
  sendTestEmail: function(toEmail) {
    var subject = '【テスト】建設業許可証管理システム テスト送信';
    var body =
      'このメールは建設業許可証管理システムのテスト送信です。\n\n' +
      '送信日時: ' + formatDate(new Date(), 'yyyy/MM/dd HH:mm:ss') + '\n\n' +
      '正常に受信できていれば、メール送信設定は正しく動作しています。';

    try {
      GmailApp.sendEmail(toEmail, subject, body);
      SpreadsheetApp.getUi().alert('テストメールを送信しました。\n宛先: ' + toEmail);
    } catch (err) {
      logError('sendTestEmail エラー', err);
      SpreadsheetApp.getUi().alert('送信に失敗しました。\n' + err.message);
    }
  },

  /**
   * 週次サマリーメールを ADMIN_EMAILS に送信する
   */
  sendWeeklySummary: function() {
    var adminEmails = getConfig('ADMIN_EMAILS');
    var enableSend = getConfig('ENABLE_SEND');
    if (!adminEmails) return;

    var permits = PermitsModel.getAllActive();
    var today = new Date();

    // 120日以内の許可証を抽出
    var nearExpiry = permits.filter(function(p) {
      var d = daysUntil(p.expiry_date);
      return !isNaN(d) && d <= 120;
    }).sort(function(a, b) {
      return daysUntil(a.expiry_date) - daysUntil(b.expiry_date);
    });

    var subject = '【週次レポート】建設業許可 期限接近一覧';

    var lines = [
      '■ 期限120日以内の許可証一覧',
      '集計日: ' + formatDate(today, 'yyyy/MM/dd'),
      '件数: ' + nearExpiry.length + '件',
      '',
      ['会社名', '許可番号', '満了日', '残日数', 'ステータス'].join('\t')
    ];

    nearExpiry.forEach(function(p) {
      var company = CompaniesModel.findById(p.company_id);
      var companyName = company ? company.company_name : p.company_id;
      var expiryStr = formatDate(
        p.expiry_date instanceof Date ? p.expiry_date : parseDate(p.expiry_date),
        'yyyy/MM/dd'
      );
      var d = daysUntil(p.expiry_date);
      var daysStr = isNaN(d) ? '不明' : (d < 0 ? '期限切れ(' + Math.abs(d) + '日経過)' : d + '日');
      lines.push([companyName, p.permit_number, expiryStr, daysStr, p.status || ''].join('\t'));
    });

    var body = lines.join('\n');

    if (enableSend === 'false') return;

    var recipients = adminEmails.split(',').map(function(e) { return e.trim(); }).filter(Boolean);
    try {
      GmailApp.sendEmail(recipients[0], subject, body, {
        cc: recipients.slice(1).join(',') || undefined
      });
    } catch (err) {
      logError('sendWeeklySummary 送信エラー', err);
    }
  }
};
