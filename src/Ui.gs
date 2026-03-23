/**
 * Ui.gs — カスタムメニューとUIハンドラ
 */

/**
 * スプレッドシートを開いたときにカスタムメニューを追加する
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('許可証管理')
    .addItem('期限チェックを今すぐ実行', 'runNow')
    .addSeparator()
    .addItem('テストメール送信', 'promptAndSendTestEmail')
    .addItem('設定チェック', 'checkConfigMenu')
    .addItem('会社ビュー更新', 'refreshCompanyViewMenu')
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
 * メニューから CompanyView シートを手動更新する
 */
function refreshCompanyViewMenu() {
  var ui = SpreadsheetApp.getUi();
  try {
    refreshCompanyView();
    ui.alert('完了', '会社ビューを更新しました。CompanyViewシートをご確認ください。', ui.ButtonSet.OK);
  } catch (err) {
    ui.alert('エラー', '会社ビューの更新中にエラーが発生しました:\n' + err.message, ui.ButtonSet.OK);
  }
}

/**
 * 全5シートのヘッダ行を一括設定する
 */
function initSheetHeaders() {
  var ui = SpreadsheetApp.getUi();
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // 既存データ確認
  var sheetsToInit = ['Config', 'Companies', 'Permits', 'Submissions', 'Notifications', 'DocumentChecklist'];
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
      'company_id', 'company_name_raw', 'company_name_normalized',
      'representative_name', 'contact_person',
      'contact_email', 'contact_email_cc', 'phone', 'status', 'created_at', 'updated_at'
    ],
    'Permits': [
      'permit_id', 'company_id', 'company_name_raw',
      'permit_authority_name', 'permit_authority_name_normalized', 'permit_authority_type', 'permit_category',
      'permit_year', 'contractor_number', 'permit_number_full',
      'trade_categories', 'issue_date', 'expiry_date', 'renewal_deadline_date',
      'current_status', 'evidence_renewal_application', 'renewal_application_date',
      'mlit_confirmed_date', 'mlit_confirm_result', 'mlit_screenshot_url',
      'permit_file_path', 'permit_file_share_url', 'permit_file_version', 'evidence_file_path',
      'last_received_date', 'source_file', 'source_file_hash',
      'parse_status', 'error_category', 'error_reason',
      'note', 'created_at', 'updated_at'
    ],
    'Submissions': [
      'submission_id', 'trigger_uid', 'submitted_at', 'company_name_raw', 'contact_email_raw',
      'permit_number_raw', 'expiry_date_raw', 'uploaded_file_drive_id',
      'uploaded_file_url', 'parsed_result', 'error_message'
    ],
    'Notifications': [
      'notification_id', 'sent_at', 'company_id', 'permit_id',
      'to_email', 'cc_email', 'stage', 'subject', 'body', 'result', 'error_message'
    ],
    'DocumentChecklist': [
      'check_id', 'company_id', 'submission_date',
      'version', 'source_message_id', 'source_attachment_id', 'source_file_hash',
      '新規継続取引申請書', '建設業許可証', '決算書前年度', '決算書前々年度',
      '会社案内', '工事経歴書', '取引先一覧表', '労働安全衛生誓約書',
      '資格略字一覧', '労働者名簿一覧表', '個人事業主_青色申告書',
      'source_pdf_path', 'llm_classification_raw', 'created_at', 'updated_at'
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
