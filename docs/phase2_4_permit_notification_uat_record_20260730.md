# Phase 2-4 許可正本・MLIT監視・通知制御 UAT実施記録

作成日: 2026-07-30

対象branch: `codex/phase0-p1-safety`

対象環境: 本番とは別のApps Script／複製Google Sheets
判定: **技術UAT・復旧演習・技術管理者OAuth PASS／業務承認・客先2アカウントUAT・本番反映は未実施**

## 1. 今回完了した範囲

- 許可正本候補と旧MLIT観測の読取専用照合。
- `PermitImportStaging`と`MonitoringTargetStaging`の追加・保護。
- Drive上の固定CSV、正規化テキストSHA-256、明示確認文字列によるprivate取込。
- 許可候補50件、監視対象131社のUAT staging取込。
- 未判断が残る状態でのdry-run停止。
- staging取込失敗時のデータ復元、正本切替失敗時のシート名復元、
  監視対象適用失敗時の会社version・監視状態復元。
- 監査ログ更新を行番号ではなく`log_id`再検索で確定する方式への修正。
- 許可期限差分、通知候補STALE化、期限切れ通知の最終段階限定、
  送信結果不明時の手動照合、低件数期間の監査付きmode昇格例外。
- 管理対象トリガーのprofile化と、無関係トリガーを削除しない制御。
- メール送信元をScript Propertiesと実行アカウントで照合する中央送信ゲート。
- レビュー台帳から承認済みstaging CSVを生成する検証ツール。
- UAT固定deployment v7の作成と、OAuth未設定時のfail-closed表示。
- GISの`origin_mismatch`を根本原因まで確認し、Authorization Code + PKCEへ変更。
- 固定deployment v8で技術管理者Googleログインと運用状態表示を確認。
- 移行後の明示チェックポイント作成と、別Google Sheetsへの復元・件数突合。

本番Apps Script、本番スプレッドシート、本番deployment、公開範囲、
トリガー、外部メール送信には変更を加えていない。

## 2. Delete Before Automateの適用

- 協力会社からのGoogleフォーム受付は現行業務に存在しないため復活させない。
- 担当者による`Companies`、`Permits`、MLIT観測、stagingの直接編集を
  恒久運用にしない。
- MLIT観測値から`Permits`正本を自動上書きしない。
- 通知候補生成とメール送信を分離し、初期は担当者確認を必須にする。
- レビュー台帳からCSVへの手転記を削除し、検証・出力ツールに一本化する。
- 承認、OAuth設定、3アカウント受入、時間経過が必要な段階昇格は
  技術作業だけで完了扱いにしない。

## 3. 許可・MLIT照合結果

生成物:

- `output/permit_master_reconciliation_20260730/permit_reconciliation.csv`
- `output/permit_master_reconciliation_20260730/permit_import_staging.csv`
- `output/permit_master_reconciliation_20260730/monitoring_target_staging.csv`
- `output/permit_master_reconciliation_20260730/permit_reconciliation_summary.json`

照合結果:

| 項目 | 件数 |
|---|---:|
| Permits | 19 |
| Permits参照会社 | 17 |
| MLITPermits | 59 |
| MLIT参照会社 | 58 |
| union会社 | 66 |
| intersection会社 | 9 |
| 照合キー | 70 |
| MATCHED | 6 |
| MLIT_ONLY | 50 |
| PERMIT_ONLY | 10 |
| INACTIVE_COMPANY | 2 |
| CONFLICT | 1 |
| DUPLICATE | 1 |
| 許可取込候補 | 50 |
| 監視対象の人手判断 | 64 |
| 監視対象の自動除外 | 67 |

人手確認が必須の代表項目:

- `PR-0048`／`C0071`／株式会社谷野宮組:
  Permits期限`2026-04-13`、MLIT観測期限`2031-04-13`。
  更新証跡を確認するまで正本へ反映しない。
- `PR-0063`／`C0118`／尾畑長硝子株式会社:
  同一upsert keyの許可が2行。残す`permit_id`を証跡で決定する。
- `PM-0015-01`／`C0028`／有限会社トーケン:
  許可区分と期限が不足。補完証拠なしの`APPROVED`は禁止する。

## 4. レビュー台帳

ファイル:

`outputs/permit_review_20260730/建設業許可_許可正本・監視対象レビュー台帳_20260730.xlsx`

構成:

- `レビュー概要`
- `判断手順`
- `許可照合`
- `許可取込候補`
- `監視対象`

検証:

- 5 sheetのXLSX round-trip import: PASS。
- 使用範囲:
  `A1:H22`、`A1:F17`、`A1:T71`、`A1:AE51`、`A1:L132`。
- 数式エラー検索: 0件。
- 5 sheetをPNG描画して目視確認。
- 概要の競合・重複は分類総数ではなく
  `classification + review_status=PENDING`の未解決件数を表示。
- 期限競合の識別子を実データどおり`PR-0048`へ修正。

承認後は次を実行する。

```powershell
python scripts\export_reviewed_permit_staging.py `
  "outputs\permit_review_20260730\建設業許可_許可正本・監視対象レビュー台帳_20260730.xlsx" `
  --output-dir "output\reviewed_permit_staging"
```

このツールは次の状態ではCSVを出力しない。

- `CONFLICT`または`DUPLICATE`がPENDING。
- 許可候補または監視対象がPENDING。
- 手動判断のreviewer、reviewed_at、review_noteが不足。
- APPROVED許可の会社ID、許可行政庁、区分、業者番号、期限が不足。
- company_id、migration_row_idが重複。
- INACTIVE会社をMONITORに指定。

成功時だけ次を出力する。

- `permit_import_staging.reviewed.csv`
- `monitoring_target_staging.reviewed.csv`
- `reviewed_staging_manifest.json`
- 各CSVの正規化テキストSHA-256。
- SHA-256に対応する`LOAD_*`確認文字列。

## 5. UAT環境

| 項目 | 値 |
|---|---|
| Spreadsheet ID | `1Hzr72GZgxtLRr1bL_SUqPSjoKEDx521b6M1IxV_y-EQ` |
| Apps Script ID | `1urr5VGVUkKXO6xV_SrSQwT3l4R1hfPH76dSdReTxp_QsszzK_5PwaMFH` |
| 固定deployment | `AKfycbwYyjjQw4ZYag37JSOexxGJHFwZG78a69-Dcmi2DcT7XoHoOAQyGTCrR5EgWbpVMm9Y2w @8` |
| 実行者 | deployment所有者 |
| アクセス | Web endpoint到達可。業務APIはGoogle OIDC + UserAccess必須 |
| `ENABLE_SEND` | `FALSE` |
| notification mode | `OFF` |
| MLIT sync mode | `OFF` |
| project trigger | 0件 |
| Google OAuth | Authorization Code + PKCE設定済み |

固定Web URL:

`https://script.google.com/macros/s/AKfycbwYyjjQw4ZYag37JSOexxGJHFwZG78a69-Dcmi2DcT7XoHoOAQyGTCrR5EgWbpVMm9Y2w/exec`

画面確認結果:

- タイトルは「建設業許可証管理システム」。
- `kalimistk@gmail.com`でGoogle OAuthを完了。
- サーバー側`UserAccess`から`technical_admin`を決定。
- 会社一覧131社を表示。
- notification mode `OFF`、MLIT mode `OFF`、managed trigger 0件。
- 送信準備完了0社、要設定131社、送信元は未設定・不一致。

認証方式と証跡の詳細:

`docs/phase0c_oauth_uat_record_20260730.md`

## 6. UAT staging取込証跡

### 許可候補CSV

- Drive file ID: `1jNrh2tzMZ_Df-PwwldZJ0Sy9L0RUMbty`
- 正規化テキストSHA-256:
  `e9f854f376cf1710bce45e15f1966a2c7ad2fdff9c76c42c0fed223f2c3dff7a`
- 確認文字列: `LOAD_PERMIT_STAGING_e9f854f376cf`

### 監視対象CSV

- Drive file ID: `1nB-zJWddWag6OcvesMPaB9oDV7Blqxj_`
- 正規化テキストSHA-256:
  `12f919e670a259dff9503e58a3e66b01f8ab377c8a5e305b7a4b4f3464791756`
- 確認文字列: `LOAD_MONITORING_STAGING_12f919e670a2`

### 実値

| sheet | 行数 | 状態 |
|---|---:|---|
| PermitImportStaging | 50 | PENDING 50 |
| MonitoringTargetStaging | 131 | PENDING 64、AUTO_APPROVED 67 |
| Companies | 131 | 変更なし |
| Permits | 19 | 変更なし |
| MLITPermits | 59 | 変更なし |
| NotificationQueue | 0 | 外部送信候補なし |

dry-runは次の理由で意図どおり停止した。

- `未判断の許可移行候補が50件あります`
- `未判断の監視対象が64社あります`

したがって`Permits`正本と`Companies.permit_monitoring_enabled`へのapplyは
実行していない。

### 監査ログ

- 初回取込失敗:
  `04c6e47b-e259-4a9c-b5f9-6500a2b6bb89`
  ／`ABORTED`
  ／`UAT_INITIAL_LOAD_AUDIT_ROW_BUG`。
- 許可staging最終取込:
  `f61e2b76-db18-438d-9f53-5c97b7c568ae`
  ／`COMMITTED`
  ／50件。
- 監視対象staging最終取込:
  `c9ec752d-fddb-4262-a9fe-4a9ea4e92541`
  ／`COMMITTED`
  ／131件。

初回失敗の根本原因は、監査追加関数の戻り値に行番号があると
呼出側が仮定していたこと。`AuditLog`は同じ`log_id`のPREPARED/COMMITTEDを
別行で保持する既存データもあるため、行番号依存を廃止し、
`updateAuditEvent_(log_id, updates)`で最新行を再取得する方式へ変更した。

一時UATランナーは最終pushから削除し、Apps Script editor上でも不存在を確認した。

## 7. 復旧・ロールバック演習

日次バックアップは同一日付の重複を作らないため、移行前に作成した
`PERMIT_BACKUP_DAILY_20260730_133800`を16:02に再実行しても
`created=false`となった。この動作は日次バックアップとして正常だが、
移行後の復旧点としては不適切である。

そのため次を実装した。

- `createRecoveryCheckpointBackup_(label, confirmation)`。
- labelは英大文字・数字・`_`・`-`の1〜32文字。
- 確認文字列は`CREATE_RECOVERY_CHECKPOINT_<label>`。
- `ScriptLock`、PREPARED/COMMITTED/ABORTED監査、35日保持。
- backup file ID、sheet名・行数・列数を監査へ保存。
- 日次と同じ`LAST_BACKUP_*`を更新し、移行前の最新復旧点にも利用可能。

UAT実行結果:

- 実行日時: 2026-07-30 16:06 JST。
- label: `POST_PHASE2_4_UAT`。
- 監査ID: `7fa2d5be-f63e-4306-9af7-c5f48307d6c0`。
- checkpoint file ID: `1lLiBLEW_KLLRVye2mZTlCXHKqxdjwMUNd85oGgSLPec`。
- file名:
  `PERMIT_BACKUP_CHECKPOINT_20260730_160616_POST_PHASE2_4_UAT`。
- 状態: `COMMITTED`。

checkpointを元UATへ上書きせず、別ファイルへコピーして復元した。

- restore spreadsheet ID:
  `1zJiuzlMOTZgEcZFMQbAaEcyvoZgmCxy0E_io9FrW4N0`
- restore URL:
  `https://docs.google.com/spreadsheets/d/1zJiuzlMOTZgEcZFMQbAaEcyvoZgmCxy0E_io9FrW4N0/edit`
- title:
  `UAT_RESTORE_DRILL_20260730_160616_POST_PHASE2_4`

復元後突合:

| sheet | データ件数 |
|---|---:|
| Companies | 131 |
| Permits | 19 |
| MLITPermits | 59 |
| PermitImportStaging | 50 |
| MonitoringTargetStaging | 131 |
| UserAccess | 3 |
| NotificationQueue | 0 |

コード側は前deployment v5、復旧機能初版v6、監査fail-closed修正版v7、
OAuth Code + PKCE版v8を保持した。
v5と当時のv7を起動し、いずれも`GOOGLE_CLIENT_ID`未設定で
fail-closedになることを確認した。Phase 2-4チェックポイントでは
一時ランナーを削除して26ファイル、OAuth追加後のv8では
`OAuthLogin.gs`を含む27ファイルであることを確認した。

## 8. 自動テスト

2026-07-30最終実行:

| suite | 結果 |
|---|---:|
| Python回帰 | 415/415 PASS |
| Phase 0送信安全 | 31/31 PASS |
| 認証・会社・通知・移行 | 38/38 PASS |
| MLIT同期・期限差分 | 13/13 PASS |
| 運用統制 | PASS |
| 数値化できるテスト合計 | 497/497 PASS |

主な追加回帰:

- 期限切れ通知の自動送信は`AUTO_ALL`だけ。
- `PENDING_RECONCILIATION`はoperations_adminだけが監査付きで確定。
- 低件数期間のmode昇格例外は10営業日、1件以上、未解決0件、理由必須。
- 許可・監視stagingの件数、PENDING、重複、必須項目を拒否。
- 監査COMMITTED失敗時に正本・version・監視状態を復元。
- version列を日付表示から整数書式`0`へ矯正。
- 設定送信元と実行アカウントが不一致ならGmailを呼ばない。
- 承認済み台帳だけがstaging CSVを生成できる。

## 9. 本番へ進めない残ゲート

1. `PR-0048`、`PR-0063`、`PM-0015-01`を含むレビュー台帳の正式判断。
2. 許可候補50件と監視対象64社のPENDING解消。
3. 承認済み台帳からCSVを生成し、別Drive file IDとhashでUATを再取込。
4. 最新UATチェックポイント後の許可正本・監視対象apply。
5. apply後に今回と同じ隔離方式で復元結果を再突合。
6. ~~UAT用Google OAuth Web clientの作成・Script Property設定。~~ **完了**
7. `m-fujita`、`kanri.tic`のGoogle identity準備と客先2role UAT。
8. PC・スマートフォンの検索、更新、競合、復帰、権限拒否確認。
9. 本番変更についての明示承認。
10. 本番反映後も`ENABLE_SEND=FALSE`、mode`OFF`、trigger 0から開始。
11. マスタ更新だけで5営業日。
12. `INTERNAL_TEST`、`MANUAL_PILOT`以降を各段階の実績で昇格。

上記は人手判断、Google Cloud設定、別アカウント操作、時間経過、本番権限を
必要とするため、今回の技術UAT完了とシステム全体完成を同一視しない。
