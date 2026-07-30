# MLIT定期確認・送信前確認 運用手順

更新日: 2026-07-30

## 現在の状態

本番はdeployment v24、公開範囲`MYSELF`、インストール型トリガー0件です。127社照合、UAT、schema移行が完了するまでトリガーを作成しません。

## データ責任

- `Companies`: 管理対象の正本。`permit_monitoring_enabled=TRUE`だけを確認する
- `Permits`: 承認済み期限の正本。通知計算はこの期限だけを使う
- `MLITPermits`: MLIT観測値、成功・失敗日時、再試行、差分確認状態

MLIT確認は`Companies + Permits`から対象を作り、観測行がなければ`MLITPermits`へseedします。MLIT観測は`Permits`を自動上書きしません。

## mode

| mode | 動作 |
|---|---|
| `OFF` | 外部MLIT確認を行わない |
| `SHADOW` | MLIT観測と差分記録だけを行う |
| `MANUAL_APPLY` | SHADOWに加え、担当者が確認した差分だけ`Permits`へ反映可能 |

modeはoperations_adminがWeb管理画面で1段階ずつ変更します。自動反映modeは実装していません。

## 管理トリガー

承認後にApps Scriptエディタから`installManagedTriggers_()`を1回だけ実行します。個別トリガーを手作業で追加しません。

| 時刻台 | handler | 内容 |
|---|---|---|
| 2時 | `runDailyBackup_` | 日次・月次バックアップ |
| 3時 | `runDailyMlitRolling_` | 全管理対象を約7日で一巡 |
| 7時 | `runPreNotificationMlitRefresh_` | 当日通知対象を送信前確認 |
| 8時 | `runDailyNotifications_` | 通知候補生成と許可status更新 |

通常確認は`ceil(管理対象許可数÷7)+2`件、最大25件です。優先予約、成功確認なし、再試行、成功日時の古い順に処理します。1件ごとのMLIT通信は共通rate limiterを通ります。

## 担当者の手動再確認

会社詳細の「MLITを再確認」は同期照会ではありません。同一許可の`refresh_requested_at`を更新し、次の3時または7時の処理枠で優先します。連絡先だけを保存した場合は予約しません。

## 失敗時の扱い

- 通信・HTTP・解析失敗: `last_attempted_at`更新、指数backoff、正本維持
- `last_success_at`と旧互換`last_synced`: MLIT取得と解析が成功した時だけ更新
- NOT_FOUND: 3回連続で初めて`PENDING_REVIEW`
- 複数候補・商号不一致・不正日付: 自動反映せず確認待ち
- 期限差分: `MLITPermits`へ記録し、通知送信を停止

## 緊急停止

1. Web管理画面で`MLIT_SYNC_MODE=OFF`
2. 必要ならScript Propertiesの`MLIT_ROLLING_PAUSE=true`
3. 異常実行が残る場合だけ対象トリガーを停止
4. Apps Script実行ログ、`MLITPermits`、`AuditLog`を保全

`ENABLE_SEND`と通知modeはメール用の独立kill switchです。MLIT停止だけでメール送信許可にはなりません。

## 監視

- `last_success_at`が7日超または空の件数
- `PENDING_REVIEW`の件数と最古`diff_detected_at`
- `consecutive_failure_count`、`consecutive_not_found_count`
- `refresh_requested_at`が残る件数
- `MLIT_SYNC_BATCH`監査のpicked件数、結果内訳、ABORTED
- 通知対象の成功確認が送信時点で24時間以内か

## 本番開始前の確認

- 127社照合承認、permit孤立参照0件
- `permit_monitoring_enabled`の対象会社を運用承認
- 複製Sheetと別deploymentでSHADOW UAT
- 商号不一致、複数候補、NOT_FOUND、日付異常の正本非破壊テスト
- 前deploymentへのコードロールバックとバックアップ復旧の実演
