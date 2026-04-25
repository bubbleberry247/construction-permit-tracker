# MLIT 毎晩ローリング再確認 - GAS Time-driven Trigger 設定手順

## 概要

GAS Web App の `runDailyMlitRolling()` を毎日 02:00 に起動し、
MLITPermits シートの許可情報をローリング再確認する。

- 1 日あたり 5 件処理（GAS 内部で約 30〜60 秒）
- 145 社を約 29 日で 1 巡
- `last_synced` が 30 日以上前 or 空の `fetch_status='OK'` 行が優先対象
- **ローカル PC は不要**（GAS = Google Apps Script はクラウドで動く）

## 設計の根拠（GPT-5.4 レビュー反映済み + Path A 採用）

| 観点 | 採用方針 |
|------|----------|
| アーキテクチャ | **Sheets 一本化**（GAS が直接 MLIT 連携、ローカル DB 経由しない） |
| データソース | `MLITPermits` シート（fetch_status / last_synced あり） |
| MLIT 連携 | `MlitSearch.gs` の既存 `searchMlitPermit_` / `fetchMlitDetail_`（UrlFetchApp） |
| MLIT 規約 | 1件ごとに `Utilities.sleep(3000)` 待機（既存 `searchMlit_` と同じ） |
| kill switch | ScriptProperties の `MLIT_ROLLING_PAUSE='true'` で即停止 |
| 同時実行制御 | `LockService.getScriptLock()` で多重起動防止（60秒待機） |
| 失敗ログ | GAS 標準ログ（Stackdriver）+ `AuditLog` シートに `MLIT_ROLLING` アクション追記 |

## time-driven trigger の登録手順（GAS エディタ）

1. ブラウザで [対象スプレッドシート](https://docs.google.com/spreadsheets/d/1FEj9OOz_NsFCDPd5eiYLrawB9x_Y5BI6cKOI3rtZlwA/edit) を開く
2. メニュー: **拡張機能** → **Apps Script**
3. 左サイドバー: **トリガー（時計アイコン）**
4. 右下: **+ トリガーを追加**
5. 設定:
   - 実行する関数: `runDailyMlitRolling`
   - イベントのソース: `時間主導型`
   - 時間ベースのトリガーのタイプ: `日付ベースのタイマー`
   - 時刻: `午前2時〜3時`
   - エラー通知設定: `今すぐ通知を受け取る`（推奨）
6. **保存**
7. 初回は OAuth スコープの承認が必要（UrlFetchApp / Spreadsheets / Properties）

### または GAS 内のコードで登録

```javascript
// Apps Script エディタから手動実行
function setupMlitRollingTrigger() {
  // 既存の同名トリガーを削除
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'runDailyMlitRolling') {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger('runDailyMlitRolling')
    .timeBased()
    .atHour(2)
    .everyDays(1)
    .create();
}
```

## 設定値の上書き（任意）

ScriptProperties に以下を設定すると、コード変更なしで挙動変更できる:

| Key | デフォルト | 意味 |
|---|---|---|
| `MLIT_ROLLING_DAILY_LIMIT` | `5` | 1回の実行で再確認する最大件数 |
| `MLIT_ROLLING_MAX_STALE_DAYS` | `30` | 最終同期からこの日数以上経過した行を対象 |
| `MLIT_ROLLING_PAUSE` | `(未設定)` | `true` で即時停止（kill switch） |

設定方法: GAS エディタ → プロジェクトの設定（歯車アイコン）→ スクリプトプロパティ

または GAS コードから:

```javascript
PropertiesService.getScriptProperties().setProperty('MLIT_ROLLING_DAILY_LIMIT', '10');
```

## 動作確認

### 候補選択を確認（dry-run 相当、MLIT は叩かない）

GAS エディタで `debugPickMlitRollingCandidates` を選んで実行:

```
（実行ログ）
候補数: 5
1: C0008 国土交通大臣 22214 last_synced=2026-03-30 02:30:04
2: C0011 愛知県知事 25392 last_synced=2026-03-30 02:30:04
...
```

### 1件だけ手動再確認

GAS エディタで `debugRefreshOneByCompanyId` を選び、エディタ内で `debugRefreshOneByCompanyId('C0011')` のような形で実行（コード変更や別関数経由）。

### バッチ全体を即時実行（trigger を待たない）

GAS エディタで `runDailyMlitRolling` を選んで実行 → ログで処理状況を確認。

### kill switch テスト

```javascript
// エディタから一時停止
pauseMlitRolling();
// runDailyMlitRolling() を実行 → 何もせず終了することを確認
runDailyMlitRolling();
// 解除
resumeMlitRolling();
```

## 緊急停止

MLIT サイトに迷惑をかけている疑いがある場合、GAS エディタから:

```javascript
pauseMlitRolling();
```

または、プロジェクトの設定 → スクリプトプロパティで `MLIT_ROLLING_PAUSE` を `true` に設定。

このフラグが立っている間:
- `runDailyMlitRolling` は起動しても何もせず終了
- ループ内（5件処理中）でも各イテレーション冒頭で再チェック → 即停止
- 既存の `runDailyNotifications` 等の他バッチには影響しない

再開: `resumeMlitRolling()` または `MLIT_ROLLING_PAUSE` プロパティを削除

## 監視ポイント

- **AuditLog シート**: `action='MLIT_ROLLING'` の行が毎日追加されるはず
  - `details` 列に `picked=N 一致=A 不一致=B 確認不可=C` の形で結果記録
- **GAS ログ（Stackdriver）**: 例外があれば `Logger.log` で記録
- **MLITPermits シート**: `last_synced` が日々更新されているか
- 30 日経っても巡回が完了しない会社が出たら `MLIT_ROLLING_DAILY_LIMIT` を増やす

## 関連ファイル

- `src/MlitRolling.gs` — 本機能の本体（runDailyMlitRolling, refreshOneMlitPermit_）
- `src/MlitSearch.gs` — MLIT 通信の下回り（searchMlitPermit_, fetchMlitDetail_）
- `src/Scheduler.gs` — 他の日次バッチ（通知）
- `src/db.gs` — 汎用 CRUD（readRecords_, updateRecord_, writeAuditLog_）

## ローカル Python 側の扱い

`src/mlit_confirm.py --rolling` および `scripts/nightly_mlit_rolling.bat`、
Windows Task Scheduler の `MLIT_Nightly_Rolling` タスクは **本機能で代替され不要**。

別タスクで以下を実施予定:
- Windows Task Scheduler の `MLIT_Nightly_Rolling` を削除
- `mlit_confirm.py --rolling` 関連コードを「初期セットアップ専用」に降格、または削除
- FastAPI の 🔄 MLITで最新化ボタンを除去（運用画面ではないため不要）
