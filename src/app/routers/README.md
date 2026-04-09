# src/app/routers/ — FastAPI ルーター

[← app/ に戻る](../README.md) | [← ルートに戻る](../../../README.md)

## 概要

FastAPI の `APIRouter` 単位でエンドポイントを分割管理する。
各ルーターは `app/database.py` の `get_db()` で SQLite 接続を取得する。

---

<!-- AUTO-GENERATED: file listing -->
## ファイル一覧

| ファイル | ルートプレフィックス | 概要 |
|----------|---------------------|------|
| `companies.py` | `/companies` | 会社一覧・詳細の取得 API（Query パラメータでフィルタ） |
| `dashboard.py` | `/dashboard` | 期限ダッシュボード — 期限切れ・接近中・有効の件数サマリー |
| `images.py` | `/images` | 許可証PDF画像のバイナリ配信（回転補正対応） |
| `pages.py` | `/pages` | ページ種類タグ管理 API（staging_viewer連携） |
| `__init__.py` | — | パッケージ初期化 |
<!-- END AUTO-GENERATED -->
