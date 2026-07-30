/**
 * Ui.gs — カスタムメニューとUIハンドラ
 */

/**
 * スプレッドシートを開いたときにカスタムメニューを追加する
 */
function onOpen_() {
  SpreadsheetApp.getUi()
    .createMenu('許可証管理')
    .addItem('期限チェックを今すぐ実行', 'runNow_')
    .addItem('設定チェック', 'checkConfigMenu_')
    .addItem('会社ビュー更新', 'refreshCompanyViewMenu_')
    .addToUi();
}

/**
 * 管理者本人が管理者宛にのみテストメールを送る。
 * Phase 0中はメニューに表示せず、末尾 "_" によりWebクライアントからも非公開。
 */
function promptAndSendTestEmail_() {
  var ui = SpreadsheetApp.getUi();
  var currentEmail = String(Session.getActiveUser().getEmail() || '').trim().toLowerCase();
  var adminEmails = normalizeEmailRecipients_(getConfig_('ADMIN_EMAILS')).map(function(email) {
    return email.toLowerCase();
  });
  if (!currentEmail || adminEmails.indexOf(currentEmail) < 0) {
    ui.alert('テストメール送信は管理者本人だけが実行できます。');
    return;
  }
  if (!isSendEnabled_()) {
    ui.alert('ENABLE_SENDが明示的なTRUEではないため、テストメールは送信できません。');
    return;
  }

  var result = ui.prompt(
    'テストメール送信',
    'ADMIN_EMAILSに登録済みの送信先メールアドレスを入力してください:',
    ui.ButtonSet.OK_CANCEL
  );

  if (result.getSelectedButton() === ui.Button.OK) {
    var email = result.getResponseText().trim();
    if (!email) {
      ui.alert('メールアドレスが入力されていません。');
      return;
    }
    if (adminEmails.indexOf(email.toLowerCase()) < 0) {
      ui.alert('外部宛先へのテスト送信は禁止されています。ADMIN_EMAILS登録先を指定してください。');
      return;
    }

    var confirmation = ui.alert(
      'テストメール送信の最終確認',
      '宛先: ' + email + '\n実メールを1通送信します。続行しますか？',
      ui.ButtonSet.YES_NO
    );
    if (confirmation !== ui.Button.YES) return;

    var sendResult = Mailer.sendTestEmail(email);
    if (sendResult && sendResult.sent) {
      ui.alert('テストメールを送信しました。\n宛先: ' + email);
    } else {
      ui.alert(
        'テストメールは送信されませんでした。\n' +
          ((sendResult && sendResult.message) || 'Notificationsと実行ログをご確認ください。')
      );
    }
  }
}

/**
 * 設定値の検証結果をダイアログ表示する
 */
function checkConfigMenu_() {
  var ui = SpreadsheetApp.getUi();
  clearConfigCache_();
  var missing = checkConfig_();
  if (missing.length === 0) {
    ui.alert('設定チェック OK', '全ての必須設定が確認できました。', ui.ButtonSet.OK);
  } else {
    ui.alert(
      '設定チェック NG',
      '以下のキーが未設定です:\n\n' + missing.join('\n') + '\n\nConfigシートをご確認ください。',
      ui.ButtonSet.OK
    );
  }
}

/**
 * メニューから CompanyView シートを手動更新する
 */
function refreshCompanyViewMenu_() {
  var ui = SpreadsheetApp.getUi();
  try {
    refreshCompanyView_();
    ui.alert('完了', '会社ビューを更新しました。CompanyViewシートをご確認ください。', ui.ButtonSet.OK);
  } catch (err) {
    ui.alert('エラー', '会社ビューの更新中にエラーが発生しました:\n' + err.message, ui.ButtonSet.OK);
  }
}
