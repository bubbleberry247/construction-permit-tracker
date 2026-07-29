# Phase 0-B / P1 隔離テスト実行記録

## 1. 実行範囲

- 実行日: 2026-07-30 JST
- 対象: Phase 0-B / P1（誤送信・外部通知起動防止）
- 本番環境: 未変更
- 本番デプロイ: 未実施
- 本番トリガー: 0件を維持
- 実メール送信: 実施していない

## 2. テスト用アーティファクト

- Spreadsheet名: `TEST_20260730_PHASE0B_建設業許可管理_TIC`
- Spreadsheet:
  https://docs.google.com/spreadsheets/d/1Hzr72GZgxtLRr1bL_SUqPSjoKEDx521b6M1IxV_y-EQ/edit
- 本番Spreadsheet IDとは別ID
- ImmutableなPhase 0-Aバックアップとは別ファイル
- Driveメタデータ最終確認:
  - `shared=false`
  - 権限は所有者1名だけ
  - 作成日時: 2026-07-30 01:07:45 JST
  - 最終更新日時: 2026-07-30 01:29:06 JST

## 3. 実行前安全条件

- `Config!B7 ENABLE_SEND`: `FALSE`
- `Config!B5 NOTIFY_STAGES_DAYS`: `90,60,30,0`
- `Config!B8 GMAIL_DAILY_LIMIT`: `150`
- Apps Scriptマニフェスト: `webapp.access=MYSELF`
- Apps Scriptトリガー: 0件
- Notifications: ヘッダーのみ
- テスト用Apps Scriptは本番とは異なるscript ID

## 4. コード反映の証跡

1. テスト用Spreadsheetの付属Apps Scriptを別ディレクトリへclone。
2. 変更ブランチ`codex/phase0-p1-safety`のApps Script 19ファイルを反映。
3. `clasp push --force`でテスト用プロジェクトだけへpush。
4. push後に別ディレクトリへfresh clone。
5. 改行コードを正規化して全19ファイルをブランチ`src`と比較。

最終結果:

- `AllNormalizedEqual=True`
- `Count=19`
- `ProbeExists=False`
- `Access=MYSELF`

## 5. カスタムメニュー実行

Spreadsheetの`許可証管理 > 期限チェックを今すぐ実行`を実行。

ダイアログ結果:

- 処理件数: 18件
- 個別エラー: 0件
- 完了

Apps Script実行履歴:

- 関数: `runNow_`
- 種類: メニュー
- 開始: 2026-07-30 01:18:58 JST
- 期間: 71.786秒
- ステータス: 完了

これにより、末尾`_`の`runNow_`をSpreadsheetカスタムメニューから実行できることを確認した。

補足:

- 初回認証確認のためApps Scriptエディタから`onOpen`を直接実行した1件は、
  `SpreadsheetApp.getUi()`をエディタ実行コンテキストから呼べないため失敗している。
- Spreadsheetを開いた際のsimple trigger `onOpen`は完了しており、カスタムメニューも正常に表示された。
- この1件は製品処理の失敗ではなく、エディタからUI専用関数を直接実行したテスト手順上の記録である。

## 6. 送信ゲート・不正設定probe

テスト用Apps Scriptだけに一時`Phase0Probe`を追加し、内部管理者宛の送信要求を
`sendSystemEmail_`へ渡した。

実行ログ:

- `enableSend=false`
- `sendResult.sent=false`
- `sendResult.blocked=true`
- `sendResult.result=BLOCKED_SEND_DISABLED`
- `9060300`は`INVALID_NOTIFY_STAGES`

Notificationsの証跡:

- 行: 2
- `company_id=PHASE0_TEST`
- `permit_id=PHASE0_TEST`
- `stage=PHASE0_GATE_PROBE`
- `result=BLOCKED_SEND_DISABLED`

Gmailの照合:

- 検索範囲: SENT
- 検索件名: `[PHASE0 TEST] SEND GATE PROBE`
- 日付条件: 2026-07-29以降
- 該当: 0件

## 7. private handlerトリガー

実行前にApps Scriptトリガー0件を確認した。

一時関数から以下を作成:

```text
ScriptApp.newTrigger('runDailyNotifications_')
  .timeBased()
  .after(60 * 1000)
  .create()
```

登録画面:

- トリガー件数: 1件
- 種類: 時間ベース
- 関数: `runDailyNotifications_`

実行履歴:

- 関数: `runDailyNotifications_`
- 種類: 時間主導型
- 開始: 2026-07-30 01:25:00 JST
- 期間: 10.26秒
- ステータス: 完了

cleanup:

- `phase0DeletePrivateTriggerProbe`で対象ハンドラだけを削除
- 実行ログ: `deleted=1`
- 最終トリガー件数: 0件

## 8. 一時コード除去

- ローカル`Phase0Probe.js`: 削除
- テスト用Apps Script`Phase0Probe.gs`: 削除
- 削除だけの差分をclaspが見落としたため、一時コメントで全19ファイル更新を発生させて余剰ファイルを除去
- 一時コメントも除去して再push
- fresh cloneで`ProbeExists=False`を確認
- fresh cloneの19ファイルが変更ブランチと全件一致

## 9. 最終安全状態

- テストSpreadsheet: 所有者のみ、`shared=false`
- `ENABLE_SEND=FALSE`
- `NOTIFY_STAGES_DAYS=90,60,30,0`
- Apps Scriptトリガー: 0件
- Apps Scriptファイル: 19件、probeなし
- Webアプリ公開範囲: `MYSELF`
- probe件名のGmail SENT: 0件
- 本番Apps Script: 未push
- 本番デプロイ: 未更新
- 本番会社マスタ: 未変更

## 10. 判定

隔離実動テストは合格。

ただし、GR-005の独立レビューは未実施であるため、本番反映は`NO-GO`のまま。
