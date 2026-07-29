# Phase 0-B / P1 本番反映記録

## 1. 結論

- 実施日: 2026-07-30 JST
- GR-005: `APPROVE / GO`
- 本番反映: 完了
- Apps Script固定バージョン: 24
- Web公開: `MYSELF`
- 実行主体: `USER_DEPLOYING`
- インストール型トリガー: 0件
- `ENABLE_SEND`: `FALSE`
- 通知メール送信: 0件
- 客先公開: 未実施
- 会社マスタ編集機能: 未実施

## 2. 対象

- 本番Apps Script:
  `1o3hSn6ae07OcIeNByJeYXEiGCJbkC5q1V5bIjmvvDDnD4Kk7gNb90Y0I`
- 更新した既存Webデプロイ:
  `AKfycbwYJIHfhZ6HBzfNlv2MRl1H3ZqaVMzYBv9KOIMebrQGJ5ftAl3rNymX1KmjjtVtagn1`
- 本番Spreadsheet:
  `1FEj9OOz_NsFCDPd5eiYLrawB9x_Y5BI6cKOI3rtZlwA`
- レビュー済みコード:
  - 実装・証跡コミット: `f595ad2`
  - GR-005承認記録コミット: `cad8582`

## 3. 反映前ゲート

- Config:
  - `ENABLE_SEND=FALSE`
  - `NOTIFY_STAGES_DAYS`は文字列`90,60,30,0`
  - `GMAIL_DAILY_LIMIT=150`
- Notifications: ヘッダのみ
- Gmail SENT検索:
  - 2026-04-01以降
  - 建設業許可、許可期限、受領確認、期限通知
  - 0件
- Webデプロイ:
  - バージョン23
  - `access=MYSELF`
  - `executeAs=USER_DEPLOYING`
- Apps ScriptバックアップZIP:
  - SHA-256:
    `6C4702EA890C21676E8B7B68D7B65A6A2F41B4F6691121B85278B4A4A75CFCFF`
  - 記録値と一致
- 本番HEAD fresh clone:
  - 19ファイル
  - バージョン23退避との差分は
    `appsscript.json`の`ANYONE → MYSELF`だけ

## 4. 直前テスト

- Phase 0-B専用Nodeテスト: 30件合格
- Python回帰テスト: 395件合格
- GAS Node VM構文: 17ファイル合格
- `git diff --check`: 合格
- Gmail送信入口: `Utils.gs`の1箇所のみ
- `clasp status`のpush対象: 19ファイル

## 5. 反映操作

1. 本番Apps Script HEADへ19ファイルを`clasp push --force`
2. 本番HEADをfresh clone
3. レビュー済みブランチのApps Script対象19ファイルと正規化比較
4. 固定バージョン24を作成
5. 既存Webデプロイをバージョン24へ更新

結果:

- push: 成功
- post-push fresh clone: 19ファイル
- ブランチ一致: `AllNormalizedEqual=True`
- version: `Created version 24`
- deployment: `@24`

## 6. 反映後ゲート

- Apps Script API:
  - Webデプロイはバージョン24
  - `access=MYSELF`
  - `executeAs=USER_DEPLOYING`
- fresh-context独立担当による本番管理画面確認:
  - バージョン24
  - アクセスできるユーザーは自分のみ
  - 実行ユーザーは自分
  - インストール型トリガー0件
- CookieなしHTTP:
  - HTTP 302
  - Googleログインへリダイレクト
- Config:
  - `ENABLE_SEND=FALSE`
  - `NOTIFY_STAGES_DAYS=90,60,30,0`
- Notifications: ヘッダのみ
- Gmail SENT検索: 対象0件
- トリガー作成操作: 0件
- 反映後のインストール型トリガー: 0件
- 会社マスタおよび本番Sheetデータ変更: 0件

## 7. 反映した安全機能

- 通知バッチ、即時実行、トリガー作成、エラー通知のprivate化
- 全メール送信を共通ゲートへ集約
- `ENABLE_SEND`厳密判定とロック取得後のConfig再読込
- Config上限とGmail実残量の両方を確認
- PENDING/SENTの冪等制御
- PENDINGをGmail前にflush
- SENT/FAILED/BLOCKEDをlock解放前にflush
- 月次`MONTHLY:YYYY-MM`キー
- 不正`NOTIFY_STAGES_DAYS`のfail-closed
- Web手動通知、公開通知API、休眠手動通知実装の削除

## 8. 反映対象外

- 客先担当者向け公開
- 会社マスタ編集画面
- Companiesシート直接編集運用の変更
- `ENABLE_SEND=TRUE`
- 通知トリガー再作成
- MLITトリガー再作成
- 実メール送信

## 9. ロールバック

異常時は以下を行う。

1. Web `MYSELF`を維持
2. `ENABLE_SEND=FALSE`を維持
3. 通知トリガーを作らない
4. 既存Webデプロイをバージョン23へ戻す
5. Gmail SENT、Notifications、AuditLogを照合する
6. 必要な範囲だけバックアップから復元する

バージョン23退避とApps Script ZIPは
`backups/phase0_20260730_002750/`に保存済み。
