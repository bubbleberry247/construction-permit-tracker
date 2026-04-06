"""
tests/test_evidence_ledger.py — evidence_ledger.py ユニットテスト

実行:
    cd <project_root>
    pytest tests/test_evidence_ledger.py -v
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定 — src/ を参照できるようにする
# ---------------------------------------------------------------------------
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from evidence_ledger import EvidenceEntry, EvidenceLedger  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_entry(
    operation: str = "ocr_extract",
    target: str = "C001",
    timestamp: str = "2026-04-06T10:00:00+00:00",
    **kwargs: object,
) -> EvidenceEntry:
    """テスト用エントリを簡易生成する。"""
    defaults = {
        "timestamp": timestamp,
        "operation": operation,
        "actor": "test",
        "target": target,
        "input_summary": "test input",
        "output_summary": "test output",
        "decision": "matched",
        "confidence": 0.9,
        "details": {},
    }
    defaults.update(kwargs)
    return EvidenceEntry(**defaults)  # type: ignore[arg-type]


@pytest.fixture()
def ledger(tmp_path: Path) -> EvidenceLedger:
    """tmp_path 配下に台帳を作成するフィクスチャ。"""
    return EvidenceLedger(tmp_path / "logs" / "evidence_ledger.jsonl")


# ===========================================================================
# record
# ===========================================================================
class TestRecord:

    def test_record_creates_file_and_appends(self, ledger: EvidenceLedger) -> None:
        """record() でファイルが作成され、エントリが追記される。"""
        entry = _make_entry()
        ledger.record(entry)

        assert ledger.path.exists()
        lines = ledger.path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1

        data = json.loads(lines[0])
        assert data["operation"] == "ocr_extract"
        assert data["target"] == "C001"

    def test_record_appends_multiple(self, ledger: EvidenceLedger) -> None:
        """複数回 record() で行が増える。"""
        ledger.record(_make_entry(target="C001"))
        ledger.record(_make_entry(target="C002"))
        ledger.record(_make_entry(target="C003"))

        lines = ledger.path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3

    def test_record_creates_parent_dirs(self, tmp_path: Path) -> None:
        """存在しない中間ディレクトリが自動作成される。"""
        deep_path = tmp_path / "a" / "b" / "c" / "ledger.jsonl"
        ledger = EvidenceLedger(deep_path)
        ledger.record(_make_entry())
        assert deep_path.exists()

    def test_record_preserves_details(self, ledger: EvidenceLedger) -> None:
        """details フィールドが正しく保存される。"""
        entry = _make_entry(details={"pages": 3, "file_hash": "abc123"})
        ledger.record(entry)

        data = json.loads(ledger.path.read_text(encoding="utf-8").strip())
        assert data["details"]["pages"] == 3
        assert data["details"]["file_hash"] == "abc123"


# ===========================================================================
# query — operation フィルタ
# ===========================================================================
class TestQueryOperation:

    def test_filter_by_operation(self, ledger: EvidenceLedger) -> None:
        """operation フィルタで該当のみ返る。"""
        ledger.record(_make_entry(operation="ocr_extract"))
        ledger.record(_make_entry(operation="sheets_upsert"))
        ledger.record(_make_entry(operation="ocr_extract"))

        results = ledger.query(operation="ocr_extract")
        assert len(results) == 2
        assert all(r.operation == "ocr_extract" for r in results)

    def test_filter_operation_no_match(self, ledger: EvidenceLedger) -> None:
        """該当なしの場合は空リスト。"""
        ledger.record(_make_entry(operation="ocr_extract"))
        results = ledger.query(operation="nonexistent")
        assert results == []


# ===========================================================================
# query — target フィルタ
# ===========================================================================
class TestQueryTarget:

    def test_filter_by_target(self, ledger: EvidenceLedger) -> None:
        """target フィルタで該当のみ返る。"""
        ledger.record(_make_entry(target="C001"))
        ledger.record(_make_entry(target="C002"))
        ledger.record(_make_entry(target="C001"))

        results = ledger.query(target="C001")
        assert len(results) == 2
        assert all(r.target == "C001" for r in results)


# ===========================================================================
# query — 日時範囲フィルタ
# ===========================================================================
class TestQueryDateRange:

    def test_filter_since(self, ledger: EvidenceLedger) -> None:
        """since 以降のエントリのみ返る。"""
        ledger.record(_make_entry(timestamp="2026-04-01T00:00:00+00:00"))
        ledger.record(_make_entry(timestamp="2026-04-05T00:00:00+00:00"))
        ledger.record(_make_entry(timestamp="2026-04-10T00:00:00+00:00"))

        results = ledger.query(since="2026-04-05T00:00:00+00:00")
        assert len(results) == 2

    def test_filter_until(self, ledger: EvidenceLedger) -> None:
        """until 以前のエントリのみ返る。"""
        ledger.record(_make_entry(timestamp="2026-04-01T00:00:00+00:00"))
        ledger.record(_make_entry(timestamp="2026-04-05T00:00:00+00:00"))
        ledger.record(_make_entry(timestamp="2026-04-10T00:00:00+00:00"))

        results = ledger.query(until="2026-04-05T00:00:00+00:00")
        assert len(results) == 2

    def test_filter_since_and_until(self, ledger: EvidenceLedger) -> None:
        """since〜until の範囲内のみ返る。"""
        ledger.record(_make_entry(timestamp="2026-04-01T00:00:00+00:00"))
        ledger.record(_make_entry(timestamp="2026-04-05T00:00:00+00:00"))
        ledger.record(_make_entry(timestamp="2026-04-10T00:00:00+00:00"))

        results = ledger.query(
            since="2026-04-03T00:00:00+00:00",
            until="2026-04-07T00:00:00+00:00",
        )
        assert len(results) == 1
        assert results[0].timestamp == "2026-04-05T00:00:00+00:00"

    def test_combined_filters(self, ledger: EvidenceLedger) -> None:
        """operation + target + 日時範囲の複合フィルタ。"""
        ledger.record(_make_entry(
            operation="ocr_extract", target="C001",
            timestamp="2026-04-05T00:00:00+00:00",
        ))
        ledger.record(_make_entry(
            operation="sheets_upsert", target="C001",
            timestamp="2026-04-05T00:00:00+00:00",
        ))
        ledger.record(_make_entry(
            operation="ocr_extract", target="C002",
            timestamp="2026-04-05T00:00:00+00:00",
        ))
        ledger.record(_make_entry(
            operation="ocr_extract", target="C001",
            timestamp="2026-04-01T00:00:00+00:00",
        ))

        results = ledger.query(
            operation="ocr_extract",
            target="C001",
            since="2026-04-03T00:00:00+00:00",
        )
        assert len(results) == 1


# ===========================================================================
# summary
# ===========================================================================
class TestSummary:

    def test_summary_counts(self, ledger: EvidenceLedger) -> None:
        """操作種別ごとの件数が正しい。"""
        ledger.record(_make_entry(operation="ocr_extract", timestamp="2026-04-01T00:00:00+00:00"))
        ledger.record(_make_entry(operation="ocr_extract", timestamp="2026-04-05T00:00:00+00:00"))
        ledger.record(_make_entry(operation="sheets_upsert", timestamp="2026-04-03T00:00:00+00:00"))

        s = ledger.summary()
        assert s["total_entries"] == 3
        assert s["operations"]["ocr_extract"]["count"] == 2
        assert s["operations"]["sheets_upsert"]["count"] == 1

    def test_summary_last_timestamp(self, ledger: EvidenceLedger) -> None:
        """最終実行日時が最も新しいタイムスタンプである。"""
        ledger.record(_make_entry(operation="ocr_extract", timestamp="2026-04-01T00:00:00+00:00"))
        ledger.record(_make_entry(operation="ocr_extract", timestamp="2026-04-05T00:00:00+00:00"))

        s = ledger.summary()
        assert s["operations"]["ocr_extract"]["last_timestamp"] == "2026-04-05T00:00:00+00:00"

    def test_summary_empty_ledger(self, ledger: EvidenceLedger) -> None:
        """空の台帳でも正常に動作する。"""
        s = ledger.summary()
        assert s["total_entries"] == 0
        assert s["operations"] == {}


# ===========================================================================
# export_csv
# ===========================================================================
class TestExportCsv:

    def test_export_csv_creates_file(self, ledger: EvidenceLedger, tmp_path: Path) -> None:
        """CSV ファイルが作成される。"""
        ledger.record(_make_entry())
        csv_path = tmp_path / "export" / "evidence.csv"
        count = ledger.export_csv(csv_path)

        assert csv_path.exists()
        assert count == 1

    def test_export_csv_headers_and_content(self, ledger: EvidenceLedger, tmp_path: Path) -> None:
        """CSV のヘッダと内容が正しい。"""
        ledger.record(_make_entry(
            operation="ocr_extract",
            target="C001",
            confidence=0.85,
            details={"pages": 2},
        ))
        csv_path = tmp_path / "evidence.csv"
        ledger.export_csv(csv_path)

        with csv_path.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["operation"] == "ocr_extract"
        assert rows[0]["target"] == "C001"
        assert rows[0]["confidence"] == "0.85"
        # details は JSON 文字列
        details = json.loads(rows[0]["details"])
        assert details["pages"] == 2

    def test_export_csv_multiple_rows(self, ledger: EvidenceLedger, tmp_path: Path) -> None:
        """複数エントリが全行出力される。"""
        for i in range(5):
            ledger.record(_make_entry(target=f"C{i:03d}"))
        csv_path = tmp_path / "evidence.csv"
        count = ledger.export_csv(csv_path)
        assert count == 5

    def test_export_csv_empty_ledger(self, ledger: EvidenceLedger, tmp_path: Path) -> None:
        """空の台帳でもヘッダ行のみのCSVが生成される。"""
        csv_path = tmp_path / "evidence.csv"
        count = ledger.export_csv(csv_path)
        assert count == 0
        assert csv_path.exists()


# ===========================================================================
# Empty / error resilience
# ===========================================================================
class TestEdgeCases:

    def test_query_empty_ledger(self, ledger: EvidenceLedger) -> None:
        """空の台帳でクエリしてもエラーにならない。"""
        assert ledger.query() == []
        assert ledger.query(operation="any") == []

    def test_query_nonexistent_file(self, tmp_path: Path) -> None:
        """ファイルが存在しなくても空リストを返す。"""
        ledger = EvidenceLedger(tmp_path / "nonexistent.jsonl")
        assert ledger.query() == []

    def test_malformed_jsonl_lines_skipped(self, ledger: EvidenceLedger) -> None:
        """不正なJSONL行があってもスキップして継続する。"""
        ledger.path.parent.mkdir(parents=True, exist_ok=True)
        with ledger.path.open("w", encoding="utf-8") as f:
            # 正常行
            f.write(json.dumps(_make_entry(target="C001").to_dict(), ensure_ascii=False) + "\n")
            # 不正行
            f.write("THIS IS NOT VALID JSON\n")
            # 空行
            f.write("\n")
            # 正常行
            f.write(json.dumps(_make_entry(target="C002").to_dict(), ensure_ascii=False) + "\n")

        results = ledger.query()
        assert len(results) == 2
        assert results[0].target == "C001"
        assert results[1].target == "C002"

    def test_confidence_none_preserved(self, ledger: EvidenceLedger) -> None:
        """confidence=None がラウンドトリップで保持される。"""
        entry = _make_entry(confidence=None)
        ledger.record(entry)

        results = ledger.query()
        assert len(results) == 1
        assert results[0].confidence is None

    def test_unicode_content(self, ledger: EvidenceLedger) -> None:
        """日本語を含むエントリが正しくラウンドトリップする。"""
        entry = _make_entry(
            input_summary="許可証PDF 2ページ",
            output_summary="愛知県知事 許可（特-6）第57805号",
            details={"会社名": "株式会社テスト建設"},
        )
        ledger.record(entry)

        results = ledger.query()
        assert len(results) == 1
        assert results[0].input_summary == "許可証PDF 2ページ"
        assert results[0].details["会社名"] == "株式会社テスト建設"
