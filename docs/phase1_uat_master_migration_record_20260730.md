# Phase 1 会社マスタ正式承認 UAT移行再現記録

## 1. 結論

- 実施日: 2026-07-30 JST
- 対象: 正式承認済み会社マスタ127行のUAT再現
- 判定: **PASS**
- 本番反映: **未実施**
- UAT Web認証: **Google OAuth Web client ID未設定のため未開始**
- 外部送信: 0件
- UATトリガー: 0件
- 本番トリガー: 0件を維持

正式承認済みstagingを、本番とは別のApps Script projectと複製
Spreadsheetへロードし、schema適用、dry-run、バックアップ、正本切替、
切替後検証を完了した。

## 2. UAT環境

- Spreadsheet:
  `TEST_20260730_PHASE0B_建設業許可管理_TIC`
- Spreadsheet ID:
  `1Hzr72GZgxtLRr1bL_SUqPSjoKEDx521b6M1IxV_y-EQ`
- Apps Script ID:
  `1urr5VGVUkKXO6xV_SrSQwT3l4R1hfPH76dSdReTxp_QsszzK_5PwaMFH`
- 固定UAT deployment:
  `AKfycbxVWFJ04Bccbzh9rExJ4aoaF9e9qG-7S4bO0qgPIkJHzneEVpQKjCWPgfQa6xUdwy2XFw`
- deployment version表示: `@2`
- Webアクセス: `MYSELF`
- 実行主体: `USER_DEPLOYING`
- UAT Drive folder:
  `1rT-AHgBtC3FXuKuLZi7s1ptos4KJ1AtN`
- UAT backup folder:
  `1UzYe0cokyQ5jTPZlzAaXjnWNh6LP2SVa`

本番識別子とはすべて異なる。実行ランナーはUATのscript IDと
Spreadsheet IDを固定照合し、一致しなければ処理を停止する構成で実行した。

## 3. コード反映

反映前のUAT Apps Script 19ファイルをcloneしてZIP退避した。

- 退避ZIP:
  `output/uat_reproduction_formal_20260730/uat_apps_script_pre_update.zip`
- SHA-256:
  `2878DED9A0AF5D6688531454A3E8FF08B66FEA9E1C717B9AE2284DAF06AE1F77`

承認済みbranchのApps Script対象25ファイルだけをUAT projectへpushした。
固定UAT deploymentを作成した後、fresh cloneして次を確認した。

- expected: 25ファイル
- remote: 25ファイル
- 改行正規化比較: 全件一致
- extra: 0
- `FormHandler`: なし

移行実行時だけUAT専用ランナーを一時追加した。実行完了後にランナーを除去し、
再度fresh cloneした。

- final remote: 25ファイル
- branch `src`との改行正規化比較: 全件一致
- extra: 0
- UATランナー: なし
- `FormHandler`: なし

## 4. 正式承認済みstaging

- Drive file ID:
  `1LVMMWmAu4F3R2ZdmbP8BRA3fB_BLBmU6`
- file name:
  `master_import_staging_approved_20260730.csv`
- MIME type: `text/csv`
- size: 42,829 bytes
- ローカルSHA-256:
  `B511F80B9FD670E35F6D258C2C04A80C805D3AC22B23A2AF99BBC810ED0BDD65`
- 原本Excel SHA-256:
  `c90fc8e5b803d2cfc47ebf7d251bdce42c09cd8978a23e99df1792d2b3d452e5`
- rows: 127
- vendor numberあり: 125
- `review_status=APPROVED`: 127
- reviewer marker: `USER_APPROVED_VIA_CODEX`
- approval time: `2026-07-30T13:10:23+09:00`

Drive metadataのsizeとローカルfile sizeが42,829 bytesで一致した。

## 5. schema・権限・dry-run

実行関数: 一時UATランナーの`uatFormalPrepare`

実行時間:

- start: 13:36:01 JST
- completed: 13:37:23 JST

結果:

- `Companies`へ不足9列を追加
- `Permits`へ`permit_data_version`を追加
- `MLITPermits`へ観測・差分管理22列を追加
- `Notifications`へ送信監査7列を追加
- `NotificationQueue`、`MasterImportStaging`を作成
- `AuditLog`へ拡張監査8列を追加
- `UserAccess`へ`canSendExternal`、`updatedBy`を追加
- 既存Companies backfill: 1行
- Permit version backfill: 19行
- MLIT観測backfill: 59行
- 主要9シートをowner管理で保護

初期role:

| email | role | active | canSendExternal |
|---|---|---:|---:|
| `m-fujita@tokai-ic.co.jp` | `master_editor` | TRUE | TRUE |
| `kanri.tic@tokai-ic.co.jp` | `operations_admin` | TRUE | TRUE |
| `kalimistk@gmail.com` | `technical_admin` | TRUE | FALSE |

dry-run:

| 項目 | 結果 |
|---|---:|
| staging | 127 |
| canonical candidate | 131 |
| 新規採番 | 64 |
| vendor numberあり | 125 |
| ACTIVE | 128 |
| INACTIVE | 3 |
| orphan company ID | 0 |
| SYSTEM_ONLY判断待ち | 0 |

SYSTEM_ONLY判断:

- `C0076=KEEP_SYSTEM_ONLY`
- `C0009=ARCHIVE_EXCLUDE`
- `C0041=ARCHIVE_EXCLUDE`
- `C0141=ARCHIVE_EXCLUDE`

監査ID:

- schema: `214a16a3-d3f2-4ab0-b4d6-b09cbe0931ca`
- staging load: `27588ea8-c2a4-4216-8306-8acfbe9b9f00`

## 6. バックアップ・正本切替

実行関数: 一時UATランナーの`uatFormalApply`

実行時間:

- start: 13:37:56 JST
- completed: 13:38:18 JST

バックアップ:

- file ID:
  `1e2Z0JMbG6_EYd7mYY6HbpCZrKjQDVQGIVyYN14HpMW8`
- file name:
  `PERMIT_BACKUP_DAILY_20260730_133800`
- owner: `kalimistk@gmail.com`
- shared: false
- backup audit ID:
  `9408f0d5-b7f2-45e0-94f2-c85cc87c8d09`
- status: `COMMITTED`

正本切替:

- 新Companies: 131行
- 旧Companies archive:
  `Companies_Archive_20260730_133809`
- migration audit ID:
  `4563b190-3d7c-4cca-a2d1-ca20357878d7`

## 7. 切替後検証

一時ランナー内の適用直後検証に加え、13:39:03〜13:39:07 JSTに
`uatFormalVerify`を独立再実行した。

| 検証項目 | 結果 |
|---|---:|
| Companies | 131 |
| ACTIVE | 128 |
| INACTIVE | 3 |
| vendor numberあり | 125 |
| company_id重複 | 0 |
| vendor_no重複 | 0 |
| Permit/MLIT孤立company_id | 0 |
| staging | 127 |
| Companies archive | 1 |
| trigger | 0 |
| Script Property `ENABLE_SEND` | FALSE |
| Config `ENABLE_SEND` | FALSE |
| notification mode | OFF |
| MLIT sync mode | OFF |
| backup status | COMMITTED |

Google Sheets connectorでも次を再確認した。

- `Companies`先頭行は`C0008`、末尾行は`C0211`
- `C0009`、`C0041`は`INACTIVE`
- staging先頭・末尾とも`APPROVED`
- reviewer markerと承認時刻を保持
- `UserAccess`の3roleと`canSendExternal`が設計どおり
- 旧Companiesはarchiveとして残存
- `Config!ENABLE_SEND=FALSE`

Apps Script管理画面でも最終トリガー0件を確認した。

## 8. 最終回帰テスト

UAT移行・証跡更新後にローカル全テストを再実行した。

- Phase 0送信安全: 30/30 PASS
- 認証・会社マスタ・通知キュー・移行: 26/26 PASS
- MLIT同期・期限差分: 13/13 PASS
- Python回帰: 404/404 PASS
- 合計: 473/473 PASS
- `git diff --check`: error 0

## 9. Web UATの現在地

固定UAT deploymentは起動し、建設業許可証管理システムの初期画面を表示した。
ただし画面には次のfail-closed表示が出る。

`Google client IDが未設定です。運用管理者へ連絡してください。`

このため、以下は未実施でありPASS扱いにしない。

- Google ID tokenによる実ログイン
- `m-fujita`、`kanri.tic`、`kalimistk`の3アカウント権限別UAT
- role偽装などの実ブラウザ攻撃テスト
- PC／スマートフォンの会社検索・更新・競合復帰

次の開始条件:

1. UAT用Google OAuth Web client IDを作成する。
2. UAT Web appの実originをAuthorized JavaScript originsへ登録する。
3. UAT Apps ScriptのScript Property `GOOGLE_CLIENT_ID`へ設定する。
4. 固定UAT deploymentを更新する。
5. 3アカウントでサインインできる状態を用意する。

## 10. 本番非変更確認

作業後に本番を読取専用で再確認した。

- 本番Spreadsheet modified time:
  `2026-07-29T17:40:01.894Z`のまま
- 本番Companies: 旧1行のまま
- 本番Config `ENABLE_SEND=FALSE`
- 本番Apps Script deployment:
  - HEAD
  - version 24
- 本番Web deployment:
  `AKfycbwYJIHfhZ6HBzfNlv2MRl1H3ZqaVMzYBv9KOIMebrQGJ5ftAl3rNymX1KmjjtVtagn1 @24`

本番Apps Scriptへのpush、本番Sheet変更、本番version/deployment更新、
トリガー作成、メール送信は実施していない。
