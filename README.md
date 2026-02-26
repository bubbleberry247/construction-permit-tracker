# 建設業許可証管理システム (construction-permit-tracker)

## システム概要

Google Sheets + Google Forms + Google Apps Script で構成する建設業許可証の期限管理システムです。
協力会社からフォームで許可証PDFを受領し、満了日に応じた自動通知・ステータス管理を行います。

## アーキテクチャ

- **Google Forms** — 協力会社が許可証PDF・情報を提出する入口
- **Google Drive** — 許可証PDFファイルを会社別フォルダで保管
- **Google Sheets** — Companies / Permits / Submissions / Notifications / Config の5シートで全データ管理
- **Google Apps Script** — フォーム受信処理・日次通知バッチ・メール送信をサーバーレスで実行
- **Gmail** — 通知メール・受領確認メール・週次サマリーの送信

## セットアップ手順

### 事前準備

- [ ] Google アカウント（G Suite / Google Workspace 推奨）
- [ ] 上記アカウントで Gmail が利用可能であること

### Step 1 — Google Drive にルートフォルダを作成

- [ ] Google Drive を開き、許可証PDFを保管するフォルダを新規作成（例: `建設業許可証`）
- [ ] フォルダを開き、URL の `folders/` 以降の文字列をコピーして **DRIVE_ROOT_FOLDER_ID** として控える
  - 例: `https://drive.google.com/drive/folders/1AbCdEfGh...` → `1AbCdEfGh...` の部分

### Step 2 — Google Sheets を新規作成

- [ ] Google Sheets で新規スプレッドシートを作成（例: `建設業許可証管理`）
- [ ] Apps Script を開く: メニュー「拡張機能」→「Apps Script」
- [ ] **Step 4** のコードをコピーしてから、メニュー「許可証管理」→「シートヘッダ初期化」を実行
- [ ] Configシートに設定値を入力（下記「Config シート設定値」参照）

### Step 3 — Google Form を作成・設定

- [ ] Google Forms で新規フォームを作成（例: `建設業許可証 提出フォーム`）
- [ ] 以下の項目を追加（項目名を正確に合わせること）:

| 項目名 | 種別 | 必須 |
|--------|------|------|
| 協力会社名 | テキスト | 必須 |
| 担当者名 | テキスト | 必須 |
| 通知先メール | テキスト | 必須 |
| 許可番号 | テキスト | 必須 |
| 許可区分（知事/大臣） | ラジオ（知事/大臣） | 必須 |
| 一般/特定 | ラジオ（一般/特定） | 必須 |
| 許可業種 | テキスト | 必須 |
| 許可年月日 | テキスト（YYYY/MM/DD） | 必須 |
| 満了日 | テキスト（YYYY/MM/DD） | 必須 |
| 許可証PDF | ファイルアップロード（PDF） | 必須 |
| 更新申請受付票PDF | ファイルアップロード（PDF） | 任意 |
| 備考 | テキスト（段落） | 任意 |

- [ ] フォームの URL から **FORM_ID** を控える
  - 例: `https://docs.google.com/forms/d/1XyZ.../viewform` → `1XyZ...` の部分

### Step 4 — Apps Script にコードをコピー

- [ ] スプレッドシートのメニュー「拡張機能」→「Apps Script」を開く
- [ ] プロジェクト名を設定（例: `建設業許可証管理`）
- [ ] `appsscript.json` の内容をマニフェストに貼り付け（「プロジェクトの設定」→「マニフェストファイルをエディタで表示」）
- [ ] 以下の `.gs` ファイルを新規ファイルとして作成し、それぞれコードを貼り付ける:
  - `Config.gs`
  - `Utils.gs`
  - `Models.gs`
  - `FormHandler.gs`
  - `Scheduler.gs`
  - `Mailer.gs`
  - `Ui.gs`

### Step 5 — スクリプトの承認・トリガー設定

- [ ] Apps Script エディタで「実行」→「関数を実行」→ `onOpen` を実行し、OAuth 承認を完了する
- [ ] スプレッドシートに戻りリロードすると「許可証管理」メニューが表示される
- [ ] 「許可証管理」→「設定チェック」で全キーが OK であることを確認
- [ ] トリガーを設定:
  - `runDailyNotifications`: 時間主導型 → 毎日（例: 午前8時〜9時）
  - `onFormSubmit`: フォーム送信時（フォームを選択）

---

## シートヘッダ（コピペ用）

### Config シート
```
key	value	description
```

### Companies シート
```
company_id	company_name	representative_name	contact_person	contact_email	contact_email_cc	phone	status	created_at	updated_at
```

### Permits シート
```
permit_id	company_id	permit_number	governor_or_minister	general_or_specific	permit_type_code	trade_categories	issue_date	expiry_date	renewal_deadline_date	status	last_received_date	last_checked_date	evidence_renewal_application	evidence_file_url	permit_file_url	permit_file_drive_id	permit_file_version	note	created_at	updated_at
```

### Submissions シート
```
submission_id	submitted_at	company_name_raw	contact_email_raw	permit_number_raw	expiry_date_raw	uploaded_file_drive_id	uploaded_file_url	parsed_result	error_message
```

### Notifications シート
```
notification_id	sent_at	company_id	permit_id	to_email	cc_email	stage	subject	body	result	error_message
```

---

## Config シート設定値

| key | value（例） | 説明 |
|-----|-------------|------|
| ADMIN_EMAILS | admin@example.com,manager@example.com | 管理者メールアドレス（カンマ区切り） |
| DRIVE_ROOT_FOLDER_ID | 1AbCdEfGhIjKlMnOpQrStUvWx | 許可証PDF保管フォルダのID |
| FORM_ID | 1XyZaBcDeFgHiJkLmNoPqRsTuV | Google Form のID |
| NOTIFY_STAGES_DAYS | 120,90,60,45,30,14,0 | 通知するステージ（満了日までの日数、カンマ区切り） |
| RUN_TIMEZONE | Asia/Tokyo | タイムゾーン |
| ENABLE_SEND | true | メール送信の有効化（false でドライラン） |

---

## 通知ステージ仕様

| ステージ | 満了日までの日数 | 送信先 | 内容 |
|----------|-----------------|--------|------|
| 120 | 約120日前 | 協力会社 | 更新準備の開始依頼 |
| 90 | 約90日前 | 協力会社 | 申請手続き開始・受付票提出依頼 |
| 60 | 約60日前 | 協力会社 | 進捗確認・受付票提出依頼 |
| 45 | 約45日前 | 協力会社 | 受付票未提出の場合は至急対応 |
| 30 | 約30日前 | 協力会社 + 管理者 | 最終警告・発注/入場への影響を警告 |
| 14 | 約14日前 | 協力会社 + 管理者 | 発注停止予告 |
| 0 | 満了当日 | 協力会社 + 管理者 | 満了日通知 |
| EXPIRED | 満了日超過 | 協力会社 + 管理者 | 期限切れ通知 |

- ステージ判定は ±1日の許容範囲あり（日次バッチ実行時刻のずれを吸収）
- 同一 permit_id + stage の SENT レコードが既存の場合は再送しない

---

## Permit ステータス一覧

| ステータス | 意味 |
|------------|------|
| VALID | 有効（満了まで30日超） |
| EXPIRING | 期限接近（満了まで30日以内） |
| RENEWAL_IN_PROGRESS | 更新申請中（受付票提出済み・満了日超過） |
| EXPIRED | 期限切れ（受付票未提出・満了日超過） |

---

## 想定トラブルと対処

- **「Configシートが見つかりません」エラー**: 「許可証管理」→「シートヘッダ初期化」を実行してシートを作成してください
- **フォーム送信後にSubmissionsにNGが記録される**: Submissionsの error_message 列を確認し、ADMIN_EMAILS に送られたエラーメールも参照してください
- **メールが届かない**: ENABLE_SEND=true になっているか確認。Notifications の result 列が DRY_RUN になっていれば false になっています
- **Gmail送信上限（1日150件）に達した**: 大量の許可証が同時期に期限を迎えている場合に発生します。ADMIN_EMAILSにアラートが届きます。翌日以降に自動で送信されます
- **トリガーが実行されない**: Apps Script の「トリガー」画面で runDailyNotifications の設定を確認してください
- **DriveApp.getFileById エラー**: フォームのファイルアップロード権限とDriveのアクセス権を確認してください
- **onFormSubmit が動作しない**: トリガーがフォーム送信時に設定されているか確認。Apps Script のダッシュボードで実行ログを確認してください
