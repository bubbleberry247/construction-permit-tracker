# 会社マスタ更新・通知自動化 実装記録

作成日: 2026-07-30
対象branch: `codex/phase0-p1-safety`
基点commit: `707794a`
状態: **会社マスタ・MLIT差分・通知連携のローカル実装、自動テスト、本番データ読取専用dry-run完了／UAT・本番反映未実施**

## 1. 結論

設計されたPhase 0-C/D、会社連絡先更新、通知キュー、手動送信、段階自動化、
バックアップ、会社マスタ移行ゲートをローカル実装した。

ただし次の理由により、客先公開・Companies正本化・外部送信はまだ実行していない。

- 本番の`GOOGLE_CLIENT_ID`が未設定。
- 本番`UserAccess`は旧`admin`の3行で、`canSendExternal`列もまだない。
- 127社dry-runには`REVIEW_REQUIRED` 13社と`SYSTEM_ONLY` 4社がある。
- 設計で必須とした複製スプレッドシート・別deploymentでの攻撃テスト／UATが未実施。
- `INTERNAL_TEST`以降の実送信、10営業日の段階昇格条件は時間経過と運用承認が必要。

本番の既存deployment v24、公開範囲`MYSELF`、トリガー0件、外部送信0件は変更していない。

## 2. 実装した機能

### 認証・権限

- 公開トップレベル関数を`doGet`と`apiDispatch`だけに制限。
- Google Identity ServicesのID tokenをブラウザメモリだけで保持。
- サーバーで`aud`、`iss`、`exp`、`email_verified`を検証。
- email、role、`canSendExternal`を`UserAccess`から決定。
- token検証失敗、未登録、inactive、権限不足はfail-closed。
- 固定キー、manual login、ブラウザ申告email／role、所有者fallbackを撤去。
- actionごとのcapability、request／payloadの余分なフィールド拒否を実装。

### 会社マスタ

- `Companies`の不足列を既存列の末尾へ非破壊追加するschema移行。
- 客先担当者の更新対象を代表者、担当者、To、CC、電話、社内担当者だけに限定。
- CC正規化・重複排除・最大5件。
- `internal_owner_email`を有効な`UserAccess`だけに限定。
- 変更理由、`OTHER`補足、差分確認を実装。
- `ScriptLock`、整数`data_version`、`request_id`冪等性を実装。
- `PREPARED`、`COMMITTED`、`ABORTED`のwrite-ahead監査を実装。
- マスタ更新時に旧`source_data_version`の通知候補を`STALE`化。
- operations_adminによるversion条件付き取消を実装。

### 通知

- 通知候補生成とGmail送信を分離。
- `NotificationQueue`に`DRAFT`、`READY`、`APPROVED`、`SENDING`、`SENT`、
  `FAILED`、`BLOCKED`、`STALE`、`CANCELLED`、`PENDING_RECONCILIATION`を実装。
- 手動送信は最大10件、定型本文は固定、追記だけ編集可能。
- Toは候補画面から変更できず、会社マスタ修正後の再生成が必要。
- すべてのGmail送信を`sendSystemEmail_`へ集約。
- 送信ロック内でmode、actor、pilot、master version、連絡先、queue状態を再検証。
- To／CC／BCCの一意受信者数で設定上限とGoogle残存quotaを判定。
- `PENDING`をGmail送信前にflushし、送信結果記録失敗時は自動再送しない。
- 不正メール形式、CR/LFを含む件名・宛先を中央ゲートで拒否。
- `OFF`から`AUTO_ALL`まで8段階のnotification modeを実装。
- mode昇格は1段階ずつ、10営業日、実送信5件、未解決0件、運用確認を要求。
- 毎日8時の処理は候補生成が先で、auto対象mode／stageだけ後段送信。

### MLIT観測・許可期限差分

- 対象選択を`MLITPermits`起点から、管理対象の`Companies + Permits`起点へ変更。
- MLIT観測行がない許可は`MLITPermits`へseedしてから確認候補にする。
- `permit_monitoring_enabled`を会社マスタに追加し、operations_adminだけが変更可能。
- MLIT確認modeを`OFF`、`SHADOW`、`MANUAL_APPLY`の3段階に限定。
- 通常確認は毎日3時台に最大25件、全体7日一巡を目標に動的件数を算出。
- 通知対象は毎日7時台に優先確認し、送信時点の成功確認24時間以内を必須化。
- 失敗時は`last_attempted_at`だけを進め、`last_success_at`と`last_synced`を進めない。
- transient failureは指数backoff、NOT_FOUNDは3回連続で初めて確認待ち。
- 期限一致、通常の5年延長、短縮、5年幅逸脱、商号不一致、複数候補を分類。
- `MLITPermits`は観測値、`Permits`は承認済み正本とし、自動上書きを禁止。
- MANUAL_APPLY時だけ担当者が期限差分を反映し、`permit_data_version`を増分。
- 期限反映時に関連する未送信通知候補を`STALE`化。
- 通知冪等キーを`permit_id + expiry_date + stage`へ変更。
- 送信直前に会社version、許可version、期限、MLIT鮮度、未解決差分を再検証。
- 期限切れ許可もMLIT巡回と`EXPIRED`通知候補生成の対象に維持。
- 送信済みの緊急stageから過去の緩いstageへ逆戻りする候補生成を禁止。
- 会社詳細に「MLITを再確認」優先予約、差分一覧、承認済み値と観測値の併記を追加。

### 受付機能の簡素化

- 協力会社からのシステム受付は現行スコープ外とし、`FormHandler.gs`を削除。
- 現行GASから受付設定、受付シートmodel、受領確認メール文言への依存を除去。
- 既存の本番シートや外部サービスはローカル実装では削除・改名していない。

### バックアップ・移行

- スプレッドシート全体の日次バックアップ、35日保持を実装。
- 毎月1日の月次バックアップ、5年保持を実装。
- 保持期限超過分は指定バックアップフォルダ内の専用prefixファイルだけゴミ箱へ移動。
- 会社マスタ適用には24時間以内の成功バックアップを必須化。
- `MasterImportStaging`へ127行だけをロードできるprivate移行入口を実装。
- 全127行`APPROVED`、業者番号一意、競合・重複0、permit孤立ID 0を再検証。
- `SYSTEM_ONLY`は会社ごとに`KEEP_SYSTEM_ONLY`または`ARCHIVE_EXCLUDE`を
  明示判断する方式へ変更。
- `ARCHIVE_EXCLUDE`はcompany_idとPermit/MLIT参照を残したまま
  Companiesの`status=INACTIVE`とし、通常画面・送信対象から除外。
- 新しい`Companies_Migration_*`を作成してから、旧`Companies`を
  `Companies_Archive_*`へ退避して切り替える。
- 移行途中のrename失敗時に旧`Companies`名を復旧する補償処理を実装。
- `Companies`、`Config`、`UserAccess`、通知・監査・staging各シートの保護を実装。

## 3. 本番データ読取専用dry-run

原本:

`C:\Users\masam\Desktop\KeyenceRK\建築許可証管理システム　業者リスト2026.7.28.xlsx`

ローカル証跡:

`C:\ProgramData\Generative AI\Github\construction-permit-tracker-phase0b\output\phase1_master_reconciliation_20260730`

ハッシュ:

- Excel原本 SHA-256:
  `c90fc8e5b803d2cfc47ebf7d251bdce42c09cd8978a23e99df1792d2b3d452e5`
- 本番Google Sheets読取専用export SHA-256:
  `d6ec97d9e4c05d7ac9a658ca6326217ee754f143f453029bc52bfc561d561486`

照合結果:

| 項目 | 件数 |
|---|---:|
| Excel会社 | 127 |
| 業者番号あり | 125 |
| 業者番号なし | 2 |
| システム上のcompany_id | 67 |
| `MATCHED` | 50 |
| `NEW_FROM_EXCEL` | 64 |
| `REVIEW_REQUIRED` | 13 |
| `SYSTEM_ONLY` | 4 |
| `DUPLICATE` | 0 |
| `CONFLICT` | 0 |
| permit参照名欠落 | 0 |

`approval_ready=false`であり、正本化は停止している。

人手確認対象:

- `master_import_staging.csv`内の`REVIEW_REQUIRED` 13行。
- `system_only.csv`内の4行:
  `C0009`、`C0041`、`C0076`、`C0141`。
- 特に`C0141`は現行Companiesのサンプル行であり、実在会社として残すかを運用判断する。

推奨判断を仮定したローカル正本化dry-run:

| 項目 | 結果 |
|---|---:|
| 正本候補 | 131社 |
| 既存company_id | 67件 |
| 新規採番 | 64件（C0148〜C0211） |
| ACTIVE | 128社 |
| INACTIVE | 3社 |
| 業者番号あり | 125社 |
| company_id重複 | 0件 |
| vendor_no重複 | 0件 |
| Permit/MLIT孤立参照 | 0件 |

SYSTEM_ONLYの推奨判断は、`C0076=KEEP_SYSTEM_ONLY`、
`C0009/C0041/C0141=ARCHIVE_EXCLUDE`。
この結果は正式承認ではなく、本番・Script Properties・Google Sheetsは未変更。

## 4. テスト結果

2026-07-30 最新実行:

- Phase 0送信安全テスト: 30/30 PASS
- 認証・マスタ・queue・移行・構文テスト: 26/26 PASS
- MLIT同期・差分・期限切れ回帰テスト: 13/13 PASS
- Python全回帰テスト: 404/404 PASS
- 今回の実行合計: 473/473 PASS
- `git diff --check`: error 0
- 全Apps Scriptファイルと`index.html`内JavaScriptの構文解析: PASS
- clasp認識対象に新規`.gs`ファイルを含むこと: 確認済み

主な攻撃・障害テスト:

- 偽role、未定義action、余分なrequest fieldを拒否。
- client ID不一致・期限切れtokenを拒否。
- 未登録・inactive利用者を拒否。
- 会社名などallowlist外更新を拒否。
- stale version、lock失敗、監査PREPARED失敗時に会社を変更しない。
- 同一request IDで二重更新しない。
- メール空欄・不正形式を送信準備未完了にする。
- stale queue、手動modeのAUTO origin、pilot外会社を拒否。
- 許可versionまたは期限が候補生成後に変わった場合の送信を拒否。
- MLIT行がない管理対象許可もCompaniesとPermitsからseed。
- transient failureと日付解析異常で正本・成功時刻を更新しない。
- 商号不一致、複数候補、3回連続NOT_FOUNDを自動反映しない。
- 通常5年更新、短縮、5年幅逸脱を分類。
- 期限切れ許可のMLIT巡回とEXPIRED候補生成を維持。
- 送信済み緊急stageから緩いstageへ逆戻りしない。
- `OFF`時の送信、重複予約、quota不足を拒否。
- 数式インジェクション、XSS用HTML挿入、メールヘッダー注入を防止。
- Gmail成功後のログ失敗時に自動再送しない。

## 5. UAT開始前の必須設定

本番ではなく、複製スプレッドシートと別Apps Script projectで先に実施する。

Script Properties:

- `GOOGLE_CLIENT_ID`: UAT用Google OAuth Web client ID
- `ENABLE_SEND`: `FALSE`
- `NOTIFICATION_MODE`: `OFF`
- `INTERNAL_TEST_RECIPIENTS`: 内部テスト許可宛先
- `PILOT_COMPANY_IDS`: 初期は空欄
- `BACKUP_FOLDER_ID`: UAT専用バックアップフォルダ

Google OAuth client:

- UAT Webアプリの実originをAuthorized JavaScript originsへ登録。
- ID tokenのaudienceと`GOOGLE_CLIENT_ID`を一致させる。

UserAccess:

- `m-fujita@tokai-ic.co.jp`: `master_editor`、active、external send可
- `kanri.tic@tokai-ic.co.jp`: `operations_admin`、active、external send可
- `kalimistk@gmail.com`: `technical_admin`、active、external send不可

初回schema:

- `operations.ensureSchema`をoperations_adminで実行。
- mutation時のconfirmationは`APPLY_SCHEMA_V2`。
- initial role、company backfill、sheet protectionを適用。

## 6. 会社マスタ移行手順

1. `master_import_staging.csv`の13件を照合する。
2. `system_only.csv`の4件を照合する。
3. `REVIEW_REQUIRED`の`matched_company_id`を承認または修正する。
4. 全127行の`review_status`を判断後に`APPROVED`へする。
5. 確定CSVをGoogle Driveへアップロードする。
6. `MASTER_STAGING_CSV_FILE_ID`へfile IDを設定する。
7. `MASTER_IMPORT_LOAD_CONFIRMATION`へ
   `LOAD_STAGING_c90fc8e5b803`を設定する。
8. `loadMasterImportStagingFromDrive_()`を実行する。
9. `MASTER_SYSTEM_ONLY_DECISIONS`へ会社ごとの判断をJSONで設定する。
    例:
    `{"C0009":"ARCHIVE_EXCLUDE","C0041":"ARCHIVE_EXCLUDE","C0076":"KEEP_SYSTEM_ONLY","C0141":"ARCHIVE_EXCLUDE"}`
10. `dryRunCanonicalMasterMigration_()`を実行し、判断待ち0件、ACTIVE/INACTIVE件数、
    company_id・vendor_no・Permit/MLIT参照を確認する。
11. `runDailyBackup_()`を実行し、成功証跡を確認する。
12. `MASTER_IMPORT_APPLY_CONFIRMATION`へ
    `APPLY_CANONICAL_c90fc8e5b803`を設定する。
13. `applyCanonicalMasterMigration_()`を実行する。
14. `Companies_Archive_*`、新`Companies`、permit孤立0、vendor重複0を確認する。

確認文字列は現在の原本hashにだけ有効である。原本が変わった場合はdry-runからやり直す。

## 7. 本番切替ゲート

次の順序を変更しない。

1. UAT用別Apps Script projectと複製Sheetを作成。
2. schema、initial role、protected settingsを適用。
3. 認証攻撃テストと3アカウント権限別UATを実施。
4. PC／スマートフォンで検索、更新、競合、復帰を確認。
5. MLIT期限差分・送信前鮮度・期限切れ回帰をUAT複製環境で確認。
6. 127社の人手照合を承認。
7. 本番バックアップを実行。
8. 本番へコード反映。ただし公開範囲と`ENABLE_SEND=FALSE`を維持。
9. 本番schemaとroleを適用。
10. Companies移行を実施。
11. Webアプリ公開範囲をGoogleアカウント必須へ変更。
12. マスタ更新だけで5営業日運用。
13. `INTERNAL_TEST`で全宛先を内部へ置換し、外部送信0を突合。
14. 連絡先を外部手段で再確認した5社だけ`MANUAL_PILOT`。
15. 各段階10営業日・5送信・事故0・未解決0・突合100%で1段階ずつ昇格。

## 8. ロールバック

- コード: v24を含む前deploymentへ戻す。
- 送信: まず管理画面でmodeを`OFF`、次にmanaged triggerを停止、最後にログ保全。
- マスタ: `Companies_Archive_*`を保全し、切替失敗時は旧シート名を`Companies`へ戻す。
- 個別変更: 現在versionが一致する場合だけ`companies.revertChange`を実行。
- Gmail成功・ログ不明: 再送せず`PENDING_RECONCILIATION`として送信済みと照合。

## 9. 未完了事項

- UAT用Apps Script project／複製Sheetの作成。
- Google OAuth Web client IDの作成・設定。
- 3アカウントの実ブラウザUAT。
- 13件＋4件の人手照合承認。
- 本番バックアップ先の設定と復旧実演。
- 本番push／deployment／公開範囲変更。
- 5営業日のマスタ更新運用。
- INTERNAL_TEST、MANUAL_PILOT以降の実運用。
- 10営業日ごとの段階昇格。

これらは別環境・人手判断・時間経過・本番変更を伴う受入ゲートである。MLIT期限差分の候補生成、担当者確認、`Permits`反映、通知候補STALE化、監査記録はローカルコードとして実装済みだが、本番トリガーは0件のままで自動再照合は稼働していない。
