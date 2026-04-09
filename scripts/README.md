# scripts/ — 実行スクリプト（Windows）

[← プロジェクトルートに戻る](../README.md)

## 概要

Windowsでのパイプライン実行・タスクスケジューラ登録用スクリプト。
本番環境での定期実行セットアップに使用する。

---

<!-- AUTO-GENERATED: file listing -->
## ファイル一覧

| ファイル | 概要 |
|----------|------|
| `run_pipeline.bat` | パイプライン一括実行バッチ（手動起動・テスト用） |
| `setup_task_scheduler.ps1` | Windowsタスクスケジューラへの定期実行登録スクリプト |
<!-- END AUTO-GENERATED -->

## 使用方法

```bat
:: パイプライン手動実行
scripts\run_pipeline.bat

:: タスクスケジューラ登録（管理者権限で実行）
powershell -ExecutionPolicy Bypass -File scripts\setup_task_scheduler.ps1
```
