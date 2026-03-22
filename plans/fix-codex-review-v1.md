# 修正プラン: Codex レビュー指摘5件の対応

## 指摘1: 同時実行でデータ破壊・二重送信
### 対象ファイル: FormHandler.gs, Scheduler.gs
### 修正内容
- `onFormSubmit()` の冒頭で `LockService.getScriptLock()` を取得（タイムアウト30秒）
- `runDailyNotifications()` の冒頭で同様にスクリプトロックを取得（タイムアウト60秒）
- ロック取得失敗時はリトライせず、エラーログ + admin通知で終了
- finally ブロックで必ず `lock.releaseLock()` を呼ぶ

## 指摘2: 他社データを上書き可能な同定ロジック
### 対象ファイル: Models.gs, FormHandler.gs
### 修正内容
- `CompaniesModel.findByNameAndEmail(companyName, contactEmail)` を新設
  - company_name（完全一致）AND contact_email 一致で同定
- `FormHandler.onFormSubmit()` の Company upsert を `findByNameAndEmail` に変更
- 既存の `findByName` は残す（Scheduler等の内部利用向け）
- 名前一致・メール不一致の場合は新規作成し、admin にアラート通知（同名別社の可能性）

## 指摘3: 通知取りこぼしが構造的に発生
### 対象ファイル: Scheduler.gs
### 修正内容
- `determineStage_()` を累積判定方式に変更:
  - stageDays を降順ソートで走査
  - `days <= stageDays[i]` かつ `NotificationsModel.hasBeenSent(permitId, stageDays[i])` が false → そのステージを返す
  - 最も大きい（最初に送るべき）未送信ステージを1つ返す
- EXPIRED も同様: `days < 0` かつ EXPIRED 未送信 → 'EXPIRED' を返す
- 1回のバッチ実行で1 permitあたり1通知のみ（最優先の未送信ステージ）

## 指摘4: 部分失敗時の整合性保証がない
### 対象ファイル: FormHandler.gs
### 修正内容
- onFormSubmit を3フェーズに分離し、各フェーズのエラーを独立処理:
  - Phase 1: データ登録（Company/Permit upsert） — 失敗時は全体NG
  - Phase 2: ファイル操作（Drive移動・リネーム） — 失敗時はログ記録して続行（既にこうなっている）
  - Phase 3: メール送信（受領確認） — 失敗時はログ記録して続行
- Permit の create/update 前に、同一 submission_id で Submissions を検索し、既に OK のものがあればスキップ（べき等性）
- SubmissionsModel に `findById(submissionId)` メソッドを追加

## 指摘5: 管理者メールアドレス漏洩
### 対象ファイル: Mailer.gs
### 修正内容
- `sendExpiryNotification()` の高アラート時 ADMIN_EMAILS を CC → BCC に変更
- GmailApp.sendEmail の options で `bcc` パラメータを使用
- company.contact_email_cc は協力会社の CC なのでそのまま CC に残す
