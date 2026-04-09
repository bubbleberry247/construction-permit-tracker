# src/app/ — FastAPI アプリケーション

[← src/ に戻る](../README.md) | [← ルートに戻る](../../README.md)

## 概要

建設業許可証管理システムのWebダッシュボード。
FastAPI + Jinja2 + SQLite で構成し、許可証の期限状況・会社一覧・証拠画像閲覧を提供する。

### エンドポイント構成

| モジュール | ルート | 用途 |
|-----------|--------|------|
| `routers/dashboard.py` | `/dashboard` | 期限一覧ダッシュボード |
| `routers/companies.py` | `/companies` | 会社一覧・詳細 API |
| `routers/images.py` | `/images` | 許可証画像配信 |
| `routers/pages.py` | `/pages` | ページ種類管理 API |

---

<!-- AUTO-GENERATED: file listing -->
## ファイル一覧

### アプリケーション基盤

| ファイル | 概要 |
|----------|------|
| `main.py` | FastAPI エントリポイント — アプリ初期化・静的ファイル・ルーター登録 |
| `database.py` | SQLite 接続管理 — `get_db()` / DB_PATH定義 |
| `__init__.py` | パッケージ初期化 |

### サブディレクトリ

| パス | 概要 |
|------|------|
| [`routers/`](routers/README.md) | FastAPI ルーター群（API定義） |
| `templates/` | Jinja2 HTMLテンプレート (`dashboard.html`, `viewer.html`, `layout.html`) |
| `static/css/` | スタイルシート (`style.css`) |
<!-- END AUTO-GENERATED -->
