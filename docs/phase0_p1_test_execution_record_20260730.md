# Phase 0-B / P1 隔離テスト実行記録

## 1. 実行範囲

- 実行日: 2026-07-30 JST
- 対象: Phase 0-B / P1（誤送信・外部通知起動防止）
- 隔離テスト中の本番環境: 未変更
- 最終本番デプロイ: 実施済み（バージョン24）
- 本番トリガー: 0件を維持
- 実メール送信: 0件

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

初回GR-005レビューは`REJECT`だったため、指摘修正と再テストを実施した。
指摘修正後の独立再レビューが未完了のため、本番反映は`NO-GO`のまま。

## 11. GR-005初回指摘の修正と再テスト

- 初回レビュー対象: `ca68e9d29fddb1608005efbfa428a6c8dd3957d8`
- 初回判定: `REJECT`
- 指摘修正コミット: `44ef025`
- 本番反映: 未実施

修正:

- `PENDING`と`SENT`を同一permit・stageの予約済みとして扱う
- `FAILED`だけを自動再試行可能とする
- 送信ゲート内で重複を再確認
- Config上限確認からGmail送信、結果更新まで共通`ScriptLock`を保持
- Schedulerの外側ロックを`DocumentLock`へ分離
- `script.send_mail` OAuth scopeを追加
- `GMAIL_DAILY_LIMIT`を必須Configへ追加
- Phase 0中のWeb手動通知ボタンと公開APIを一時除去

ローカル再テスト:

- Phase 0-B専用テスト: 23件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check`: 成功

追加した重要テスト:

- Gmail送信成功後に`SENT`更新が失敗し`PENDING`が残るケース
- 上記の次回実行でGmail呼び出し0件
- `FAILED`は再試行可能
- 共通`ScriptLock`を上限確認から結果更新まで保持
- 競合実行でGmail呼び出し1件

## 12. `script.send_mail`隔離quota probe

実行先は既存の所有者限定テスト用Apps Scriptのみ。本番にはpushしていない。

一時関数`phase0QuotaScopeProbe`は、次だけを実行した。

1. `ENABLE_SEND=FALSE`を確認
2. `MailApp.getRemainingDailyQuota()`を呼び出す
3. 結果をconsoleへ記録

送信APIは呼び出していない。

実行ログ:

```text
{"enableSend":false,"remainingDailyRecipientQuota":100,"sent":false}
実行完了
```

cleanupと最終照合:

- 一時`Phase0QuotaProbe`をテスト用Apps Scriptから除去
- fresh clone: 19ファイル
- `ProbeExists=False`
- ブランチ`src`との正規化比較: `AllNormalizedEqual=True`
- Webアプリ: `MYSELF`
- 最終トリガー: 0件
- 本番Apps Script: 未変更

## 13. 現在の判定

修正と隔離再テストは合格。指摘修正後のfresh-context独立再レビュー待ち。
再レビューが`APPROVE`になるまで本番反映は`NO-GO`。

## 14. 第2回GR-005レビュー

- 対象: `575a93e`
- 判定: `APPROVE_WITH_CHANGES`
- 本番反映可否: `CONDITIONAL_GO`
- Critical: 0件
- 初回Critical/Warning: 解消を確認

追加Warning:

1. 送信ロック取得後の`ENABLE_SEND`再確認がConfigキャッシュを再利用する。
2. permit IDが空の月次サマリーは`PENDING`残留時の重複防止対象外。

第2回レビュアーの独立テスト:

- Phase 0-B専用テスト: 23件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check`: 成功
- Gmail送信箇所: `Utils.gs`の1箇所のみ

## 15. 第2回指摘の修正

- 修正コミット: `0baca3f`
- `reloadConfigAll_()`で送信ロック取得後にConfigを強制再読込
- `ENABLE_SEND`と`GMAIL_DAILY_LIMIT`を同じfresh snapshotから判定
- 月次サマリーへ`MONTHLY:YYYY-MM`の月別冪等キーを付与
- 休眠中の`sendManualNotification_`実装を削除

追加テスト:

- 実際の`Config.gs`を読み込み、ロック待機中の`TRUE→FALSE`でGmail呼び出し0件
- 月次`PENDING`残留後の同月再実行でGmail呼び出し0件
- `ERROR_ALERT`は繰り返し可能イベントとして2回送信可能

再テスト:

- Phase 0-B専用テスト: 26件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check`: 成功

所有者限定テスト環境の再同期:

- Apps Scriptへ19ファイルをpush
- fresh clone: 19ファイル
- probeファイル: なし
- ブランチ`src`との正規化比較: `AllNormalizedEqual=True`
- 本番Apps Script: 未変更

## 16. 現在の判定

第2回指摘修正後のfresh-context最終レビュー待ち。
最終レビューが`APPROVE`になるまで本番反映は`NO-GO`。

## 17. 第3回GR-005レビュー

- 対象: `c4b69a2`
- 判定: `APPROVE_WITH_CHANGES`
- 本番反映可否: `CONDITIONAL_GO`
- Critical: 0件
- 第2回Warning: 解消を確認

残ったWarning:

- NotificationsのPENDING/SENT/FAILED/BLOCKEDを`SpreadsheetApp.flush()`せず、
  `ScriptLock`を解放している。

第3回レビュアーの独立テスト:

- Phase 0-B専用テスト: 26件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check`: 成功
- Gmail送信箇所: `Utils.gs`の1箇所のみ

## 18. flush修正

- 修正コミット: `09ae67f`
- PENDING作成直後、Gmail送信前に`SpreadsheetApp.flush()`
- pre-send flush失敗時はGmail呼び出し0件
- SENT/FAILED更新後、ロック解放前にflush
- BLOCKED系も`finally`でロック解放前にflush
- 送信後flush失敗時も、送信前に確定済みのPENDINGが自動再送を停止

追加テスト:

- PENDING flush失敗時にGmail呼び出し0件
- PENDING flushがGmail前、結果flushがロック解放前
- 遅延commit模擬でも確定済みPENDINGが次回送信を停止
- 月が変われば別の月次冪等キーで送信可能

再テスト:

- Phase 0-B専用テスト: 30件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check`: 成功

所有者限定テスト環境の再同期:

- Apps Scriptへ19ファイルをpush
- fresh clone: 19ファイル
- probeファイル: なし
- ブランチ`src`との正規化比較: `AllNormalizedEqual=True`
- 本番Apps Script: 未変更

## 19. 現在の判定

flush修正後のfresh-context最終レビューを完了。

## 20. GR-005最終レビュー

- 対象: `f595ad2`
- 判定: `APPROVE`
- 本番反映可否: `GO`
- Critical: 0件
- Warning: 0件
- 対象範囲: Phase 0-B安全機能のみ
- 維持条件:
  - `ENABLE_SEND=FALSE`
  - Webアプリ`MYSELF`
  - 通知トリガー0件
  - 客先公開、会社マスタ編集、送信有効化、実メール送信を行わない

最終レビュアーの独立確認:

- Phase 0-B専用テスト: 30件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check c4b69a2..f595ad2`: 成功
- Gmail送信箇所: `Utils.gs`の1箇所のみ
- FAILED/BLOCKED、post-send flush失敗・再試行、予期しない例外を
  追加fault injectionし、flush後のlock解放と自動再送防止を確認

## 21. 現在の判定

GR-005は`APPROVE / GO`。
本番直前条件の再確認後、Phase 0-B安全機能だけを反映できる。

## 22. 本番直前確認

- 実施日時: 2026-07-30 02:35〜02:40 JST
- Config:
  - `ENABLE_SEND=FALSE`
  - `NOTIFY_STAGES_DAYS`は文字列`90,60,30,0`
  - `GMAIL_DAILY_LIMIT=150`
- Notifications: ヘッダのみ
- Gmail SENT検索:
  - 2026-04-01以降
  - 建設業許可、許可期限、受領確認、期限通知
  - 0件
- 既存Webデプロイ:
  - バージョン23
  - `access=MYSELF`
  - `executeAs=USER_DEPLOYING`
- CookieなしHTTPアクセス: Googleログインへ302
- Apps ScriptバックアップZIP:
  - SHA-256:
    `6C4702EA890C21676E8B7B68D7B65A6A2F41B4F6691121B85278B4A4A75CFCFF`
  - 記録値と一致
- 本番HEAD fresh clone: 19ファイル
- バージョン23退避ソースとの差分:
  - `appsscript.json`の`ANYONE → MYSELF`のみ
  - Phase 0-A封じ込めの意図した差分

直前再テスト:

- Phase 0-B専用テスト: 30件合格
- 既存Python回帰テスト: 395件合格
- GAS構文: 17ファイル成功
- `git diff --check`: 成功
- push対象: 19ファイル

## 23. 本番反映

- 実施日時: 2026-07-30 02:40〜02:41 JST
- 本番Apps Script HEADへ19ファイルをpush: 成功
- push後fresh clone: 19ファイル
- レビュー済みブランチとの正規化比較: `AllNormalizedEqual=True`
- 作成バージョン: 24
- 説明: `v24: Phase 0-B notification safety gate`
- 更新した既存デプロイ:
  `AKfycbwYJIHfhZ6HBzfNlv2MRl1H3ZqaVMzYBv9KOIMebrQGJ5ftAl3rNymX1KmjjtVtagn1`
- デプロイ更新結果: `@24`

## 24. 本番反映後確認

- Apps Script API:
  - アクティブWebデプロイ: バージョン24
  - `access=MYSELF`
  - `executeAs=USER_DEPLOYING`
- fresh-context独立担当による本番管理画面再確認:
  - バージョン24
  - アクセスできるユーザー: 自分のみ
  - 次のユーザーとして実行: 自分
  - インストール型トリガー: 0件
- CookieなしHTTPアクセス:
  - HTTP 302
  - `accounts.google.com`へリダイレクト
- Config:
  - `ENABLE_SEND=FALSE`
  - `NOTIFY_STAGES_DAYS=90,60,30,0`
- Notifications: ヘッダのみ
- Gmail SENT検索: 対象0件
- 客先公開: 未実施
- 会社マスタ編集機能: 未実施
- 送信有効化: 未実施
- 実メール送信: 0件

## 25. 現在の判定

- Phase 0-A: 完了
- Phase 0-B / P1: 本番反映完了
- 本番Webデプロイ: バージョン24、所有者本人のみ
- インストール型トリガー: 0件
- 顧客担当者向け公開: 未許可
- 会社マスタ更新: 未許可
- 通知メール: 停止継続
