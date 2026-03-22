# 修正プラン v2: Codex レビュー指摘5件の対応（Codexプランレビュー反映済み）

## Codex プランレビュー指摘への対応
- 指摘A: ScriptLockで両処理を排他すると、バッチ長期実行中にフォーム処理が取りこぼされる
  → FormHandler と Scheduler で別々のロックを使い分ける（後述）
- 指摘B: submission_id は毎回 generateUuid() で新規生成されるためべき等性キーにならない
  → フォームイベントの triggerUid（e.triggerUid）をべき等性キーとして使用する

---

## 指摘1: 同時実行でデータ破壊・二重送信
### 対象ファイル: FormHandler.gs, Scheduler.gs
### 修正内容
- FormHandler: `LockService.getDocumentLock()` を使用（タイムアウト30秒）
  - ドキュメントロックなのでスクリプトロックとは独立
  - フォーム同時投稿の排他のみが目的
- Scheduler: `LockService.getScriptLock()` を使用（タイムアウト60秒）
  - バッチの多重実行防止が目的
- **重要**: FormHandler と Scheduler は別ロックなので互いにブロックしない
- ロック取得失敗時:
  - FormHandler: ログ記録 + admin通知（フォーム送信データはSheets上に残るため手動復旧可能）
  - Scheduler: ログ記録のみ（次回バッチで再処理される設計）
- finally ブロックで必ず releaseLock()

## 指摘2: 他社データを上書き可能な同定ロジック
### 対象ファイル: Models.gs, FormHandler.gs
### 修正内容
- `CompaniesModel.findByNameAndEmail(companyName, contactEmail)` を新設
  - company_name（完全一致、大文字小文字無視）AND contact_email 一致で同定
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
### 対象ファイル: FormHandler.gs, Models.gs
### 修正内容
- べき等性キーとして `e.triggerUid`（GASフォームトリガーが提供する一意ID）を使用
  - triggerUid が取得できない場合（手動テスト等）は generateUuid() にフォールバック
- `SubmissionsModel.findByTriggerUid(triggerUid)` を新設
  - Submissions シートに `trigger_uid` カラムを追加
- onFormSubmit 冒頭で triggerUid による既存チェック:
  - parsed_result='OK' の既存レコードがあればスキップ（重複防止）
  - parsed_result='NG'/'PROCESSING' があれば再処理（リカバリ）
- onFormSubmit を3フェーズに分離し、各フェーズのエラーを独立処理:
  - Phase 1: データ登録（Company/Permit upsert） — 失敗時は全体NG
  - Phase 2: ファイル操作（Drive移動・リネーム） — 失敗時はログ記録して続行（既存動作維持）
  - Phase 3: メール送信（受領確認） — 失敗時はログ記録して続行（既存動作維持）

## 指摘5: 管理者メールアドレス漏洩
### 対象ファイル: Mailer.gs
### 修正内容
- `sendExpiryNotification()` の高アラート時 ADMIN_EMAILS を CC → BCC に変更
- ccList から adminEmails を分離し、bccList として管理
- company.contact_email_cc は協力会社の CC なのでそのまま CC に残す
- GmailApp.sendEmail の options: { cc: contact_email_cc, bcc: adminEmails }

---

## 変更対象ファイル・カラム一覧
| ファイル | 変更種別 |
|----------|----------|
| FormHandler.gs | ロック追加、Company同定変更、triggerUid対応、フェーズ分離 |
| Scheduler.gs | ロック追加、determineStage_ 累積判定化 |
| Models.gs | findByNameAndEmail追加、findByTriggerUid追加 |
| Mailer.gs | CC→BCC変更 |
| Ui.gs | initSheetHeaders に trigger_uid カラム追加 |
