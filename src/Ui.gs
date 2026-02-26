/**
 * Ui.gs — カスタムメニューとUIハンドラ
 */

/**
 * スプレッドシートを開いたときにカスタムメニューを追加する
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('許可証管理')
    .addItem('期限チェックを今すぐ実行', 'Scheduler.runNow')
    .addSeparator()
    .addItem('テストメール送信', 'promptAndSendTestEmail')
    .addItem('設定チェック', 'checkConfigMenu')
    .addSeparator()
    .addItem('シートヘッダ初期化', 'initSheetHeaders')
    .addToUi();
}

/**
 * メールアドレスを入力してテストメール送信する
 */
function promptAndSendTestEmail() {
  var ui = SpreadsheetApp.getUi();
  var result = ui.prompt(
    'テストメール送信',
    '送信先メールアドレスを入力してください:',
    ui.ButtonSet.OK_CANCEL
  );

  if (result.getSelectedButton() === ui.Button.OK) {
    var email = result.getResponseText().trim();
    if (!email) {
      ui.alert('メールアドレスが入力されていません。');
      return;
    }
    Mailer.sendTestEmail(email);
  }
}

/**
 * 設定値の検証結果をダイアログ表示する
 */
function checkConfigMenu() {
  var ui = SpreadsheetApp.getUi();
  clearConfigCache();
  var missing = checkConfig();
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
 * 全5シートのヘッダ行を一括設定する
 */
function initSheetHeaders() {
  var ui = SpreadsheetApp.getUi();
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // 既存データ確認
  var sheetsToInit = ['Config', 'Companies', 'Permits', 'Submissions', 'Notifications'];
  var hasData = false;
  sheetsToInit.forEach(function(name) {
    var s = ss.getSheetByName(name);
    if (s && s.getLastRow() > 1) hasData = true;
  });

  if (hasData) {
    var resp = ui.alert(
      'シートヘッダ初期化',
      'データが存在するシートがあります。\nヘッダ行（1行目）のみを上書きします。データは削除されません。\n\n続行しますか？',
      ui.ButtonSet.YES_NO
    );
    if (resp !== ui.Button.YES) return;
  }

  var SHEET_HEADERS = {
    'Config': ['key', 'value', 'description'],
    'Companies': [
      'company_id', 'company_name', 'representative_name', 'contact_person',
      'contact_email', 'contact_email_cc', 'phone', 'status', 'created_at', 'updated_at'
    ],
    'Permits': [
      'permit_id', 'company_id', 'permit_number', 'governor_or_minister',
      'general_or_specific', 'permit_type_code', 'trade_categories',
      'issue_date', 'expiry_date', 'renewal_deadline_date', 'status',
      'last_received_date', 'last_checked_date', 'evidence_renewal_application',
      'evidence_file_url', 'permit_file_url', 'permit_file_drive_id',
      'permit_file_version', 'note', 'created_at', 'updated_at'
    ],
    'Submissions': [
      'submission_id', 'trigger_uid', 'submitted_at', 'company_name_raw', 'contact_email_raw',
      'permit_number_raw', 'expiry_date_raw', 'uploaded_file_drive_id',
      'uploaded_file_url', 'parsed_result', 'error_message'
    ],
    'Notifications': [
      'notification_id', 'sent_at', 'company_id', 'permit_id',
      'to_email', 'cc_email', 'stage', 'subject', 'body', 'result', 'error_message'
    ]
  };

  var errors = [];

  // Config シートの初期値（シートが空の場合のみ追記）
  var CONFIG_DEFAULTS = [
    ['ADMIN_EMAILS',        '',               '管理者メールアドレス（カンマ区切り）'],
    ['DRIVE_ROOT_FOLDER_ID','',               '許可証PDF保管フォルダのID'],
    ['FORM_ID',             '',               'Google Form のID'],
    ['NOTIFY_STAGES_DAYS',  '120,90,60,45,30,14,0', '通知するステージ（満了日までの日数）'],
    ['RUN_TIMEZONE',        'Asia/Tokyo',     'タイムゾーン'],
    ['ENABLE_SEND',         'false',          'メール送信有効化（trueで送信）'],
    ['GMAIL_DAILY_LIMIT',   '150',            'Gmail日次送信上限']
  ];

  Object.keys(SHEET_HEADERS).forEach(function(sheetName) {
    try {
      var sheet = ss.getSheetByName(sheetName);
      if (!sheet) {
        sheet = ss.insertSheet(sheetName);
      }
      var headers = SHEET_HEADERS[sheetName];
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

      // ヘッダ行を太字・背景色で見やすくする
      var headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setFontWeight('bold');
      headerRange.setBackground('#e8f0fe');

      // Config シートの初期値を追記（データ行が無い場合のみ）
      if (sheetName === 'Config' && sheet.getLastRow() <= 1) {
        sheet.getRange(2, 1, CONFIG_DEFAULTS.length, CONFIG_DEFAULTS[0].length)
             .setValues(CONFIG_DEFAULTS);
      }
    } catch (err) {
      errors.push(sheetName + ': ' + err.message);
    }
  });

  if (errors.length > 0) {
    ui.alert('一部エラー', '以下のシートでエラーが発生しました:\n' + errors.join('\n'), ui.ButtonSet.OK);
  } else {
    ui.alert('完了', '全シートのヘッダを初期化しました。', ui.ButtonSet.OK);
  }
}
