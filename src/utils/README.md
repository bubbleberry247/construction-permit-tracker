# src/utils/ — 共通ユーティリティ

[← src/ に戻る](../README.md) | [← ルートに戻る](../../README.md)

## 概要

OCR後処理・日本語フィールド正規化・建設業ドメイン知識を集約するユーティリティ群。
他のスクリプト (`ocr_permit.py`, `reconcile.py` 等) から `import` して利用する。

---

<!-- AUTO-GENERATED: file listing -->
## ファイル一覧

| ファイル | 概要 |
|----------|------|
| `jp_field_pack.py` | OCR後処理の日本語フィールド正規化統合パック — 許可番号・日付・業種等の一括正規化 |
| `permit_parser.py` | 許可番号文字列のパーサ — `知事許可（般-5）第12345号` 形式を構造化データに変換 |
| `trade_master.py` | 建設業許可 29業種マスタ + OCR表記ゆれ正規化（建設業法 別表第一） |
| `wareki_convert.py` | 和暦（令和・平成・昭和）→ 西暦変換（許可証OCR後処理用） |
| `email_utils.py` | 転送メールから元送信者メールアドレスを抽出するユーティリティ |
| `__init__.py` | パッケージ初期化 |
<!-- END AUTO-GENERATED -->
