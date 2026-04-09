# tests/ — テスト・ベンチマーク

[← プロジェクトルートに戻る](../README.md)

## 概要

コアロジックのユニットテストおよびOCRベンチマーク。
`pytest` で実行する。テスト対象モジュールは `src/` 配下。

### 実行方法

```bash
# 全テスト実行
pytest tests/ -v

# 特定テストのみ
pytest tests/test_core.py -v
```

---

<!-- AUTO-GENERATED: file listing -->
## ファイル一覧

| ファイル | テスト対象 | 概要 |
|----------|-----------|------|
| `test_core.py` | コアロジック全般 | 建設業許可証管理システム コアロジック ユニットテスト |
| `test_reconcile.py` | `src/reconcile.py` | 145社照合ロジックのユニットテスト |
| `test_evidence_ledger.py` | `src/evidence_ledger.py` | 監査証跡台帳のユニットテスト |
| `test_jp_field_pack.py` | `src/utils/jp_field_pack.py` | 日本語フィールド正規化のユニットテスト |
| `test_rpa_drift_watchdog.py` | `src/rpa_drift_watchdog.py` | ドリフト検知ウォッチドッグのユニットテスト |
| `test_session_briefing.py` | `src/session_briefing.py` | セッションブリーフィングのユニットテスト |
| `test_wiki_lint.py` | `src/wiki_lint.py` | Wikiドキュメント lint のユニットテスト |
| `ocr_benchmark.py` | OCR処理 | OCR精度・速度ベンチマーク（pytest対象外、単独実行） |
<!-- END AUTO-GENERATED -->
