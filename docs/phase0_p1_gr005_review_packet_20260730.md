# Phase 0-B / P1 GR-005 レビューパケット

## 1. 判定対象

- 対象システム: 建設業許可管理_TIC
- 対象フェーズ: Phase 0-B / P1（誤送信・外部通知起動防止）
- 作業ブランチ: `codex/phase0-p1-safety`
- ベースコミット: `73d8828d2ea70eb18c63692bc8bfef53a5201688`
- 初回レビュー対象コミット: `ca68e9d29fddb1608005efbfa428a6c8dd3957d8`
- 指摘修正コミット: `44ef025`
- 作成日: 2026-07-30 JST
- 初回レビュー判定: **REJECT**
- 現在のレビュー判定: **PENDING（指摘修正後のfresh-context再レビュー待ち）**
- 隔離テスト反映: **実施済み**
- 本番反映: **未実施**

このレビューは、客先公開や会社マスタ編集機能を許可するものではありません。

## 2. 基準コードの照合

以下の3つが全19ファイルで一致することを確認しました。

1. 本番アクティブデプロイのバージョン23を `clasp pull --versionNumber 23` したソース
2. Apps Scriptプロジェクトの現在HEAD
3. Gitの `origin/master`（`73d8828`）

したがって、Phase 0-B差分に未反映コードが混入する状態ではありません。

## 3. 本番の封じ込め状態

Phase 0-Aで以下を確認済みです。

- Webアプリ: 所有者本人のみ
- アクティブデプロイ: バージョン23
- 時限トリガー: 0件
- `ENABLE_SEND`: `FALSE`
- Phase 0作業中の対象通知メール送信: 0件
- SheetおよびApps Scriptバックアップ: 取得済み

Phase 0-B実装・隔離テスト中に本番環境では以下を行っていません。

- 本番Apps Scriptへの`clasp push`
- Apps Scriptバージョン作成
- デプロイ更新
- 本番トリガー作成
- メール送信
- 本番Google Sheetsのデータ変更
- 会社マスタ取込

隔離テスト環境では、所有者だけが閲覧できるコピーへコードをpushし、一回限りの
`runDailyNotifications_`トリガーを作成・実行・削除しました。テスト用Notificationsには
`BLOCKED_SEND_DISABLED`の証跡行を1件残しています。

## 4. 実装内容

### P1-1: エラー通知の非公開化

- `sendErrorAlert` を `sendErrorAlert_` へ変更
- 全呼び出し元を同時更新
- エラー通知も共通送信ゲートを通過
- 管理者宛先は先頭をTo、残りをBCCとして扱う
- `ENABLE_SEND`が明示的なTRUEでなければ送信しない

### P1-2: 通知バッチの非公開化

- `runDailyNotifications` を `runDailyNotifications_` へ変更
- `runNow` を `runNow_` へ変更
- `setupDailyTrigger` を `setupDailyTrigger_` へ変更
- 将来のトリガーハンドラ名を `runDailyNotifications_` に変更
- `runNow_` はバッチ結果を受けてからSpreadsheet UIへ成功・停止を表示
- Phase 0中はテストメール、ヘッダ初期化、トリガー再作成をカスタムメニューに表示しない

Apps Script公式仕様では末尾 `_` のサーバー関数は `google.script.run` から呼び出せません。
隔離テストでは、末尾 `_` の関数がApps Scriptエディタの実行関数一覧に表示されないこと、
Spreadsheetカスタムメニューから`runNow_`を実行できること、および
`ScriptApp.newTrigger('runDailyNotifications_')`で作成した時間トリガーが
`runDailyNotifications_`を完了できることを実測確認しました。

### P1-3: 送信ゲートの一本化

全 `GmailApp.sendEmail` を `Utils.gs` の `sendSystemEmail_` 1箇所へ集約しました。

送信条件と処理境界:

1. `ENABLE_SEND`をtrim・大文字化した結果が厳密に `TRUE`
2. Toが設定済み
3. 共通`ScriptLock`を取得
4. `GMAIL_DAILY_LIMIT`が1以上の整数
5. 同一permit・stageの`PENDING`または`SENT`が存在しない
6. 当日の`PENDING`＋`SENT`が設定上限未満
7. `MailApp.getRemainingDailyQuota()`が全受信者数以上
8. 送信前の`PENDING`記録に成功
9. Gmail送信とNotifications結果更新を完了

手順3〜9は同じ`ScriptLock`保持中に直列実行します。Schedulerのバッチ多重実行防止は
`DocumentLock`へ分離し、送信ゲート内の`ScriptLock`と自己デッドロックしない構成です。

重複判定では`PENDING`と`SENT`を予約済みとして扱い、`FAILED`だけを再試行可能とします。
Gmail送信成功後に`SENT`更新が失敗して`PENDING`が残っても、次回処理では同じ
permit・stageを自動再送しません。`PENDING`はGmail送信済みとの手動照合対象です。

いずれかを満たさない場合は送信せず、`BLOCKED_*`をNotificationsまたは実行ログへ残します。

対象経路:

- 期限通知
- 受領確認通知
- 月次サマリー
- テストメール
- 内部エラー通知

Phase 0中は不要な誤操作面を削るため、Web UIの「今すぐ通知送信」と
公開`apiSendNotification`入口を一時的に除去しました。内部の手動通知ロジックを将来
再有効化する場合も、共通送信ゲートを通る設計です。

送信後のNotifications更新だけが失敗した場合、メール送信自体を失敗扱いにしません。これにより、再実行による二重送信判断を防ぎます。

### P1-4: 通知日数の厳密検証

共通関数 `parseNotifyStages_` を追加しました。

- 形式: `^\d{1,3}(,\d{1,3})*$`
- 範囲: 0〜365
- 重複: 除去
- 並び: 降順
- 空欄・未設定・不正値: 拒否
- `9060300`: 拒否

不正時:

- 日次バッチはPermitを1件も取得・処理しない
- 内部エラーメールも送らない
- 受領確認と手動通知は`BLOCKED_INVALID_STAGES`を記録

### P1-5: Gmail実クォータ

- Config上限だけでなく `MailApp.getRemainingDailyQuota()` を送信直前に確認
- 明示的なOAuth scopeへ`https://www.googleapis.com/auth/script.send_mail`を追加
- Gmail実残量は受信者数ベースで評価
- To/CC/BCCの重複を除いた一意受信者数を必要量として計算
- 送信ループ途中でも`PENDING`＋`SENT`が設定上限へ達した時点で停止
- `GMAIL_DAILY_LIMIT`を`checkConfig()`の必須キーへ追加

## 5. 変更ファイル

- `package.json`
- `src/CompanyViewModel.gs`
- `src/Config.gs`
- `src/FormHandler.gs`
- `src/Mailer.gs`
- `src/Models.gs`
- `src/Scheduler.gs`
- `src/Ui.gs`
- `src/Utils.gs`
- `src/api.gs`
- `src/appsscript.json`
- `src/index.html`
- `src/logic.gs`
- `tests/test_phase0_p1.js`
- `docs/phase0_p1_gr005_review_packet_20260730.md`
- `docs/phase0_p1_test_execution_record_20260730.md`

## 6. 自動検証

### Phase 0-B専用テスト

実行:

```text
npm run test:phase0-p1
```

結果:

- 23件合格
- 実メール送信なし

主な検証:

- `TRUE` / `true`だけ送信許可
- `FALSE` / `false` / 空欄 / 未設定 / `1` / その他を拒否
- `9060300`を拒否
- 重複通知日数の除去と降順化
- Config上限不正時の送信停止
- Gmail実残量不足・取得失敗時の送信停止
- エラー通知も共通ゲートを迂回しない
- PENDING記録失敗時に送信しない
- Gmail送信例外をFAILEDへ更新
- 送信成功後のログ更新失敗を送信失敗と誤判定しない
- 送信成功後に`PENDING`が残っても次回Gmail呼び出し0件
- `FAILED`は再試行可能
- 上限確認から結果更新まで共通`ScriptLock`を保持
- 競合実行時のGmail呼び出しが1件だけ
- 日次上限到達後の次送信を停止
- 不正通知日数時にPermit処理を開始しない
- `GmailApp.sendEmail`が共通ゲート1箇所だけ
- 危険な通知入口が公開トップレベル関数として残っていない
- Webの手動通知ボタンと公開APIが存在しない
- Phase 0中の危険メニューが非表示
- Scheduler外側ロックと送信ゲートのロック種別が分離
- `script.send_mail` scopeがマニフェストに存在
- `GMAIL_DAILY_LIMIT`が必須設定
- Webアプリ公開範囲が`MYSELF`から戻らない

### 既存回帰テスト

実行:

```text
python -X utf8 -m pytest -q
```

結果:

- 395件合格

### 構文・差分

- GAS 17ファイルをNode VMで構文コンパイル: 成功
- `git diff --check`: 成功
- `GmailApp.sendEmail`: `src/Utils.gs`の1箇所のみ

### 隔離実動テスト

- テストSheet: `TEST_20260730_PHASE0B_建設業許可管理_TIC`
- 共有状態: `shared=false`、所有者1名のみ
- Config: `ENABLE_SEND=FALSE`、`NOTIFY_STAGES_DAYS=90,60,30,0`
- Apps Scriptソース: fresh cloneした19ファイルがブランチの`src`と全件一致
- Webアプリマニフェスト: `access=MYSELF`
- `runNow_`カスタムメニュー実行: 18件処理、個別エラー0件、完了
- `9060300`: `INVALID_NOTIFY_STAGES`で拒否
- 送信probe: `BLOCKED_SEND_DISABLED`、`sent=false`
- Notifications: `BLOCKED_SEND_DISABLED`を1件記録
- Gmail SENT検索: probe件名の該当0件
- private handlerトリガー:
  - 関数: `runDailyNotifications_`
  - 種類: 時間主導型
  - 開始: 2026-07-30 01:25:00 JST
  - 期間: 10.26秒
  - ステータス: 完了
- cleanup: 対象トリガー1件削除、最終トリガー0件
- 一時`Phase0Probe`ファイル: 除去済み
- `script.send_mail`隔離quota probe:
  - `enableSend=false`
  - `remainingDailyRecipientQuota=100`
  - `sent=false`
  - 実行完了
- quota probe cleanup:
  - 一時`Phase0QuotaProbe`ファイルを除去
  - fresh cloneは19ファイル
  - `ProbeExists=False`
  - ブランチ`src`との正規化比較`AllNormalizedEqual=True`
  - 最終トリガー0件

詳細証跡は`docs/phase0_p1_test_execution_record_20260730.md`を参照してください。

## 7. 初回GR-005レビューと修正

初回fresh-contextレビューはコミット`ca68e9d`を`REJECT`、本番`NO-GO`と判定しました。

指摘と対応:

1. Critical: Gmail送信成功後の`SENT`更新失敗で`PENDING`が残り、Schedulerの
   `hasBeenSent()`が`SENT`だけを見るため二重送信し得る。
   - 対応: `PENDING`と`SENT`を予約済みとして扱う
   - 中央送信ゲートでもpermit・stage重複を再確認
   - 専用の再送防止テストを追加
2. Warning: Config上限確認、quota確認、PENDING作成、送信が共通排他下にない。
   - 対応: 全送信経路を共通`ScriptLock`で直列化
   - Scheduler外側を`DocumentLock`へ変更
   - 競合実行テストを追加
3. Warning: `MailApp.getRemainingDailyQuota()`に必要な`script.send_mail` scopeがない。
   - 対応: マニフェストへscopeを追加
   - テスト環境で無送信quota probeを実行
4. Suggestion: `GMAIL_DAILY_LIMIT`が`checkConfig()`の必須キーにない。
   - 対応: 必須キーへ追加
5. Suggestion: Phase 0中にもWeb手動通知入口が残る。
   - 対応: ボタンと公開APIを一時除去

修正コミットは`44ef025`です。修正後のfresh-context再レビューが完了するまで
本番判定は`NO-GO`を維持します。

## 8. Configセルをプレーンテキスト化する非破壊手順

本番ではGR-005承認後にのみ実施します。

1. Configシートのバックアップと実値を再確認する。
2. `NOTIFY_STAGES_DAYS`の該当セル1個だけを選択する。
3. 表示形式を「書式なしテキスト」にする。
4. 既存文字列 `90,60,30,0` を同じ値で再入力する。
5. 表示値が厳密に `90,60,30,0` であることを確認する。
6. 隣接セル、行数、キー列、他Config値が変わっていないことを確認する。
7. 不一致があればデプロイせずバックアップ値へ戻す。

行全体・列全体への表示形式変更、`clearContents()`、Config再生成は行いません。

## 9. 本番反映前に未確認の事項

以下が残っているため、現時点ではGR-005をAPPROVEにしません。

1. 指摘修正後のfresh-context独立再レビュー
2. 再レビュー指摘がある場合の修正と再テスト
3. 本番反映直前のConfig、Gmail送信済み、トリガー0件、バックアップ再確認
4. 本番反映後も`ENABLE_SEND=FALSE`・Webアプリ`MYSELF`を維持したまま行う最終確認

## 10. 隔離テスト手順と実績

1. ImmutableなPhase 0-Aバックアップとは別に、テスト専用Spreadsheetコピーを作る。**完了**
2. コピーの共有範囲を所有者だけにする。**完了**
3. `ENABLE_SEND=FALSE`を厳密に確認する。**完了**
4. インストール型トリガーが0件であることを確認する。**完了**
5. このブランチのコードをテスト用Apps Scriptへ反映する。**完了**
6. 日次処理を手動実行し、処理件数・エラー件数を確認する。**18件・0件で完了**
7. 内部管理者宛の送信probeを実行し、BLOCKED記録とGmail SENT 0件を確認する。**完了**
8. `9060300`が`INVALID_NOTIFY_STAGES`で拒否されることを確認する。**完了**
9. テスト専用の一時関数から、`runDailyNotifications_`を指す短時間トリガーを作成する。**完了**
10. トリガー実行記録を確認後、トリガーと一時関数を削除する。**完了**
11. テスト用プロジェクトのソースがこのブランチ差分と一致することを再確認する。**完了**
12. 初回独立レビュー。**REJECT**
13. 指摘修正、23件の専用テスト、395件の回帰テスト、quota probe。**完了**
14. 指摘修正後の独立再レビュー。**未実施**

## 11. GR-005再判定欄

レビュアーは以下を1つ選択します。

- `APPROVE`: 指摘なし。本番反映ゲートへ進める
- `APPROVE_WITH_CHANGES`: 指摘修正と再テスト後に進める
- `REJECT`: P1設計または実装の再検討が必要

記入項目:

- 判定:
- レビュアー:
- 日時:
- 対象コミット:
- Critical:
- Warning:
- Suggestion:
- 本番反映可否:

## 12. 本番ロールバック

ロールバック条件:

- `ENABLE_SEND`禁止時にメール送信
- 不正通知日数で通知処理開始
- 通知バッチのWeb起動成功
- 未認証または意図しないユーザーによる送信起動
- PENDING/SENT記録の重大不整合

手順:

1. Webアプリを「自分のみ」のまま維持
2. トリガー0件を維持
3. アクティブデプロイをバージョン23へ戻す
4. `ENABLE_SEND=FALSE`を再確認
5. Gmail送信済み、Notifications、AuditLogを照合
6. 必要な場合のみPhase 0-Aバックアップから復元

## 13. 公式仕様参照

- Apps Script private functions:
  - https://developers.google.com/apps-script/guides/html/communication#private_functions
- Installable triggers:
  - https://developers.google.com/apps-script/guides/triggers/installable
- `ScriptApp.newTrigger(functionName)`:
  - https://developers.google.com/apps-script/reference/script/script-app#newTrigger(String)
- `MailApp.getRemainingDailyQuota()`:
  - https://developers.google.com/apps-script/reference/mail/mail-app#getRemainingDailyQuota()
