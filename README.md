# 建設業許可証管理システム

## 現行スコープ

Google Apps Script WebアプリとGoogle Sheetsで、会社連絡先・建設業許可期限・MLIT公表情報・通知候補を管理します。協力会社からのシステム受付は行いません。

- 客先担当者はWeb画面から会社連絡先だけを更新する
- `Companies`の直接編集、客先担当者による会社追加・削除・会社名変更は行わない
- `Companies`を管理対象と連絡先の正本、`Permits`を承認済み許可期限の正本とする
- `MLITPermits`はMLIT観測値と差分確認状態を保持し、`Permits`を自動上書きしない
- 通知は`NotificationQueue`で担当者が確認し、中央送信ゲートを通ったものだけ送る

## 主要シート

| シート | 役割 |
|---|---|
| `Companies` | 会社ID、業者番号、連絡先、管理対象フラグ、version |
| `Permits` | 承認済み許可情報と`permit_data_version` |
| `MLITPermits` | MLIT観測、成功・失敗日時、再試行、差分状態 |
| `NotificationQueue` | 未送信候補、会社・許可version、期限、状態 |
| `Notifications` | 送信予約・送信結果と冪等キー |
| `UserAccess` | サーバー側roleと外部送信権限 |
| `AuditLog` | 認証、更新、差分反映、送信、設定変更の監査 |

## 認証と安全停止

- Google ID tokenをサーバーで検証し、roleは`UserAccess`から決定する
- 公開サーバー関数は`doGet`と`apiDispatch`だけ
- `ENABLE_SEND=TRUE`かつ通知mode・権限・宛先・version・MLIT鮮度・quotaの全条件を満たさない限り送信しない
- MLIT確認modeは`OFF`、`SHADOW`、`MANUAL_APPLY`だけ
- 本番の会社マスタ127社照合と孤立参照0件が確認できるまで公開・トリガー作成を行わない

## MLIT確認と通知フロー

1. 毎日3時台に、管理対象の`Companies + Permits`から古い許可を最大25件確認する
2. 毎日7時台に、通知対象となる許可だけを送信前確認する
3. 一致時は観測成功日時だけを更新する
4. 期限差分、商号不一致、複数候補、3回連続NOT_FOUNDは確認待ちにする
5. 担当者が差分を承認した場合だけ`Permits`を更新し、関連通知候補を`STALE`にする
6. 8時台に通知候補を生成し、初期運用では担当者が最大10件ずつ確認して送る

会社詳細の「MLITを再確認」は即時外部照会ではなく、次のMLIT処理枠への優先予約です。画面の「最終成功確認日時」はMLIT公表情報を取得できた時刻であり、行政側の公表遅延までは保証しません。

## 開発と検証

```bash
npm install
npm run test:gas
python -m pytest tests/test_master_reconciliation.py -q
```

`npm run push`、deployment作成、`installManagedTriggers_`実行、`ENABLE_SEND`変更は、本番移行ゲートの承認後に別手順で行います。

## 主要ディレクトリ

| ディレクトリ | 内容 |
|---|---|
| `src/` | GAS、Python処理、Webアプリ |
| `tests/` | GAS安全テスト、MLIT同期テスト、マスタ照合テスト |
| `docs/` | 設計、レビュー、本番証跡 |
| `plans/` | 設計・移行計画 |
| `policy/` | データ不変条件 |
