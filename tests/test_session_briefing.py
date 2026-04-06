"""
tests/test_session_briefing.py -- session_briefing モジュールのユニットテスト

テスト対象:
  - generate_briefing: DB情報の集計
  - format_briefing_text: マークダウン出力
  - format_briefing_json: JSON出力
  - 各 _count_* / _check_warnings ヘルパー

実行:
    cd <project_root>
    pytest tests/test_session_briefing.py -v
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定
# ---------------------------------------------------------------------------
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from session_briefing import (  # noqa: E402
    BriefingReport,
    _check_warnings,
    _count_companies,
    _count_expiring,
    _count_pending_ocr,
    _count_recent_submissions,
    _count_unmatched,
    _get_last_pipeline_run,
    format_briefing_json,
    format_briefing_text,
    generate_briefing,
)


# ---------------------------------------------------------------------------
# テスト用 DDL (最小限)
# ---------------------------------------------------------------------------

_TEST_DDL = """
CREATE TABLE companies (
    company_id   TEXT PRIMARY KEY,
    official_name TEXT NOT NULL,
    name_aliases  TEXT,
    corporation_type TEXT,
    status        TEXT DEFAULT 'ACTIVE',
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE TABLE permits (
    permit_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id    TEXT NOT NULL REFERENCES companies(company_id),
    permit_number TEXT,
    permit_authority TEXT,
    permit_category TEXT,
    permit_year   TEXT,
    issue_date    TEXT,
    expiry_date   TEXT,
    current_flag  INTEGER NOT NULL DEFAULT 1,
    source        TEXT DEFAULT 'ocr',
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE TABLE inbound_messages (
    message_id      TEXT PRIMARY KEY,
    company_id      TEXT REFERENCES companies(company_id),
    sender_email    TEXT,
    original_sender TEXT,
    received_at     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE TABLE pages (
    page_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id       INTEGER,
    company_id    TEXT,
    page_no       INTEGER NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE TABLE ocr_runs (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id       INTEGER REFERENCES pages(page_id),
    run_type      TEXT NOT NULL DEFAULT 'initial',
    status        TEXT DEFAULT 'completed',
    started_at    TEXT,
    finished_at   TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

CREATE TABLE jobs (
    job_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    started_at    TEXT,
    finished_at   TEXT,
    result_summary TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mem_conn() -> sqlite3.Connection:
    """インメモリSQLiteにテスト用テーブルを作成して返す"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_TEST_DDL)
    return conn


@pytest.fixture
def db_file(tmp_path: Path) -> Path:
    """tmp_pathにテスト用DBファイルを作成して返す"""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_TEST_DDL)
    conn.close()
    return db_path


def _insert_company(
    conn: sqlite3.Connection, cid: str, name: str = "テスト会社"
) -> None:
    conn.execute(
        "INSERT INTO companies (company_id, official_name) VALUES (?, ?)",
        (cid, name),
    )


def _insert_permit(
    conn: sqlite3.Connection,
    company_id: str,
    expiry_date: str,
    current_flag: int = 1,
) -> None:
    conn.execute(
        "INSERT INTO permits (company_id, expiry_date, current_flag) "
        "VALUES (?, ?, ?)",
        (company_id, expiry_date, current_flag),
    )


def _insert_message(
    conn: sqlite3.Connection,
    msg_id: str,
    company_id: str | None,
    received_at: str,
) -> None:
    conn.execute(
        "INSERT INTO inbound_messages (message_id, company_id, received_at) "
        "VALUES (?, ?, ?)",
        (msg_id, company_id, received_at),
    )


def _insert_page(conn: sqlite3.Connection, page_id: int) -> None:
    conn.execute(
        "INSERT INTO pages (page_id, page_no) VALUES (?, 1)",
        (page_id,),
    )


def _insert_ocr_run(conn: sqlite3.Connection, page_id: int) -> None:
    conn.execute(
        "INSERT INTO ocr_runs (page_id, status) VALUES (?, 'completed')",
        (page_id,),
    )


def _insert_job(
    conn: sqlite3.Connection,
    status: str = "completed",
    finished_at: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO jobs (job_type, status, finished_at) "
        "VALUES ('pipeline', ?, ?)",
        (status, finished_at),
    )


# ===========================================================================
# _count_companies
# ===========================================================================
class TestCountCompanies:

    def test_empty_db(self, mem_conn: sqlite3.Connection) -> None:
        total, with_p, without_p = _count_companies(mem_conn)
        assert total == 0
        assert with_p == 0
        assert without_p == 0

    def test_all_with_permits(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        _insert_company(mem_conn, "C002")
        today = date.today()
        future = (today + timedelta(days=200)).isoformat()
        _insert_permit(mem_conn, "C001", future)
        _insert_permit(mem_conn, "C002", future)
        total, with_p, without_p = _count_companies(mem_conn)
        assert total == 2
        assert with_p == 2
        assert without_p == 0

    def test_mixed(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        _insert_company(mem_conn, "C002")
        _insert_company(mem_conn, "C003")
        _insert_permit(mem_conn, "C001", "2026-12-31")
        total, with_p, without_p = _count_companies(mem_conn)
        assert total == 3
        assert with_p == 1
        assert without_p == 2

    def test_non_current_permit_excluded(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        """current_flag=0 の許可証は除外"""
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-12-31", current_flag=0)
        total, with_p, without_p = _count_companies(mem_conn)
        assert total == 1
        assert with_p == 0
        assert without_p == 1


# ===========================================================================
# _count_expiring
# ===========================================================================
class TestCountExpiring:

    def test_empty_db(self, mem_conn: sqlite3.Connection) -> None:
        result = _count_expiring(mem_conn)
        assert result == {"expired": 0, "30d": 0, "60d": 0, "90d": 0}

    def test_expired_permit(self, mem_conn: sqlite3.Connection) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-04-01")  # 5日前
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["expired"] == 1
        assert result["30d"] == 0

    def test_30d_bucket(self, mem_conn: sqlite3.Connection) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-04-20")  # 14日後
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["30d"] == 1
        assert result["expired"] == 0

    def test_60d_bucket(self, mem_conn: sqlite3.Connection) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-05-20")  # 44日後 → 60dバケット
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["60d"] == 1
        assert result["30d"] == 0

    def test_90d_bucket(self, mem_conn: sqlite3.Connection) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-06-20")  # 75日後 → 90dバケット
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["90d"] == 1
        assert result["60d"] == 0

    def test_beyond_90d_not_counted(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-12-31")  # 269日後
        result = _count_expiring(mem_conn, reference_date=ref)
        assert all(v == 0 for v in result.values())

    def test_boundary_exactly_on_ref_date(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        """expiry_date == reference_date は expired"""
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-04-06")
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["expired"] == 1

    def test_uncertain_excluded(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "UNCERTAIN")
        result = _count_expiring(mem_conn)
        assert all(v == 0 for v in result.values())

    def test_non_current_excluded(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_permit(mem_conn, "C001", "2026-04-01", current_flag=0)
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["expired"] == 0

    def test_multiple_permits(self, mem_conn: sqlite3.Connection) -> None:
        ref = date(2026, 4, 6)
        _insert_company(mem_conn, "C001")
        _insert_company(mem_conn, "C002")
        _insert_company(mem_conn, "C003")
        _insert_permit(mem_conn, "C001", "2026-04-01")   # expired
        _insert_permit(mem_conn, "C002", "2026-04-20")   # 30d
        _insert_permit(mem_conn, "C003", "2026-05-20")   # 60d
        result = _count_expiring(mem_conn, reference_date=ref)
        assert result["expired"] == 1
        assert result["30d"] == 1
        assert result["60d"] == 1
        assert result["90d"] == 0


# ===========================================================================
# _count_recent_submissions
# ===========================================================================
class TestCountRecentSubmissions:

    def test_empty(self, mem_conn: sqlite3.Connection) -> None:
        assert _count_recent_submissions(mem_conn) == 0

    def test_within_7_days(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        recent = (date.today() - timedelta(days=3)).isoformat()
        _insert_message(mem_conn, "m1", "C001", recent)
        assert _count_recent_submissions(mem_conn) == 1

    def test_older_excluded(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        old = (date.today() - timedelta(days=10)).isoformat()
        _insert_message(mem_conn, "m1", "C001", old)
        assert _count_recent_submissions(mem_conn, days=7) == 0

    def test_custom_days(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        d = (date.today() - timedelta(days=25)).isoformat()
        _insert_message(mem_conn, "m1", "C001", d)
        assert _count_recent_submissions(mem_conn, days=30) == 1
        assert _count_recent_submissions(mem_conn, days=7) == 0


# ===========================================================================
# _count_pending_ocr
# ===========================================================================
class TestCountPendingOcr:

    def test_empty(self, mem_conn: sqlite3.Connection) -> None:
        assert _count_pending_ocr(mem_conn) == 0

    def test_page_without_ocr(self, mem_conn: sqlite3.Connection) -> None:
        _insert_page(mem_conn, 1)
        assert _count_pending_ocr(mem_conn) == 1

    def test_page_with_ocr(self, mem_conn: sqlite3.Connection) -> None:
        _insert_page(mem_conn, 1)
        _insert_ocr_run(mem_conn, 1)
        assert _count_pending_ocr(mem_conn) == 0

    def test_mixed(self, mem_conn: sqlite3.Connection) -> None:
        _insert_page(mem_conn, 1)
        _insert_page(mem_conn, 2)
        _insert_page(mem_conn, 3)
        _insert_ocr_run(mem_conn, 1)  # page 1 のみ OCR 済
        assert _count_pending_ocr(mem_conn) == 2


# ===========================================================================
# _get_last_pipeline_run
# ===========================================================================
class TestGetLastPipelineRun:

    def test_no_jobs(self, mem_conn: sqlite3.Connection) -> None:
        assert _get_last_pipeline_run(mem_conn) is None

    def test_completed_job(self, mem_conn: sqlite3.Connection) -> None:
        _insert_job(mem_conn, "completed", "2026-04-05T07:00:00")
        assert _get_last_pipeline_run(mem_conn) == "2026-04-05T07:00:00"

    def test_pending_job_excluded(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        _insert_job(mem_conn, "pending", None)
        assert _get_last_pipeline_run(mem_conn) is None

    def test_latest_selected(self, mem_conn: sqlite3.Connection) -> None:
        _insert_job(mem_conn, "completed", "2026-04-01T07:00:00")
        _insert_job(mem_conn, "completed", "2026-04-05T07:00:00")
        assert _get_last_pipeline_run(mem_conn) == "2026-04-05T07:00:00"


# ===========================================================================
# _count_unmatched
# ===========================================================================
class TestCountUnmatched:

    def test_empty(self, mem_conn: sqlite3.Connection) -> None:
        assert _count_unmatched(mem_conn) == 0

    def test_matched(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        _insert_message(mem_conn, "m1", "C001", "2026-04-01")
        assert _count_unmatched(mem_conn) == 0

    def test_unmatched(self, mem_conn: sqlite3.Connection) -> None:
        _insert_message(mem_conn, "m1", None, "2026-04-01")
        assert _count_unmatched(mem_conn) == 1

    def test_mixed(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        _insert_message(mem_conn, "m1", "C001", "2026-04-01")
        _insert_message(mem_conn, "m2", None, "2026-04-02")
        _insert_message(mem_conn, "m3", None, "2026-04-03")
        assert _count_unmatched(mem_conn) == 2


# ===========================================================================
# _check_warnings
# ===========================================================================
class TestCheckWarnings:

    def test_empty_db_has_no_run_warning(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        warnings = _check_warnings(mem_conn)
        assert any("実行履歴" in w for w in warnings)

    def test_expired_warning(self, mem_conn: sqlite3.Connection) -> None:
        _insert_company(mem_conn, "C001")
        past = (date.today() - timedelta(days=10)).isoformat()
        _insert_permit(mem_conn, "C001", past)
        warnings = _check_warnings(mem_conn)
        assert any("期限切れ" in w for w in warnings)

    def test_pending_ocr_warning(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        _insert_page(mem_conn, 1)
        warnings = _check_warnings(mem_conn)
        assert any("OCR未処理" in w for w in warnings)

    def test_unmatched_warning(self, mem_conn: sqlite3.Connection) -> None:
        _insert_message(mem_conn, "m1", None, "2026-04-01")
        warnings = _check_warnings(mem_conn)
        assert any("未紐付け" in w for w in warnings)

    def test_missing_data_root_warning(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        fake_path = Path("/nonexistent/path/to/data")
        warnings = _check_warnings(mem_conn, data_root=fake_path)
        assert any("データディレクトリ" in w for w in warnings)

    def test_no_warnings_when_healthy(
        self, mem_conn: sqlite3.Connection
    ) -> None:
        """正常状態: 期限切れなし、OCR済、メール紐付け済、パイプライン実行済"""
        _insert_company(mem_conn, "C001")
        future = (date.today() + timedelta(days=200)).isoformat()
        _insert_permit(mem_conn, "C001", future)
        _insert_page(mem_conn, 1)
        _insert_ocr_run(mem_conn, 1)
        _insert_message(mem_conn, "m1", "C001", date.today().isoformat())
        now_iso = date.today().isoformat() + "T07:00:00"
        _insert_job(mem_conn, "completed", now_iso)
        warnings = _check_warnings(mem_conn)
        assert warnings == []


# ===========================================================================
# generate_briefing (統合テスト)
# ===========================================================================
class TestGenerateBriefing:

    def test_empty_db(self, db_file: Path) -> None:
        report = generate_briefing(db_file)
        assert report.total_companies == 0
        assert report.companies_with_permits == 0
        assert report.companies_without_permits == 0
        assert report.expired == 0
        assert report.generated_at  # ISO 8601 が設定されていること

    def test_with_data(self, db_file: Path) -> None:
        conn = sqlite3.connect(str(db_file))
        _insert_company(conn, "C001", "テスト建設A")
        _insert_company(conn, "C002", "テスト建設B")
        _insert_company(conn, "C003", "テスト建設C")
        future = (date.today() + timedelta(days=200)).isoformat()
        _insert_permit(conn, "C001", future)
        _insert_permit(conn, "C002", future)
        _insert_message(conn, "m1", "C001", date.today().isoformat())
        _insert_message(conn, "m2", None, date.today().isoformat())
        _insert_page(conn, 1)
        _insert_job(conn, "completed", date.today().isoformat() + "T07:00")
        conn.commit()
        conn.close()

        report = generate_briefing(db_file)
        assert report.total_companies == 3
        assert report.companies_with_permits == 2
        assert report.companies_without_permits == 1
        assert report.recent_submissions == 2
        assert report.pending_ocr == 1
        assert report.unmatched_emails == 1
        assert report.last_pipeline_run is not None

    def test_db_not_found(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.db"
        with pytest.raises(FileNotFoundError):
            generate_briefing(missing)


# ===========================================================================
# format_briefing_text
# ===========================================================================
class TestFormatBriefingText:

    def _sample_report(self) -> BriefingReport:
        return BriefingReport(
            generated_at="2026-04-06T10:00:00",
            total_companies=145,
            companies_with_permits=98,
            companies_without_permits=47,
            expiring_90d=12,
            expiring_60d=8,
            expiring_30d=5,
            expired=3,
            recent_submissions=4,
            pending_ocr=2,
            last_pipeline_run="2026-04-05T07:00:00",
            unmatched_emails=1,
            warnings=["期限切れが3社あります", "OCR未処理ファイルが2件あります"],
        )

    def test_contains_header(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "# Session Briefing" in text

    def test_contains_date(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "2026-04-06" in text

    def test_contains_company_count(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "145社" in text
        assert "98社" in text
        assert "47社" in text

    def test_contains_expiry_counts(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "期限切れ: 3社" in text
        assert "30日以内: 5社" in text
        assert "60日以内: 8社" in text
        assert "90日以内: 12社" in text

    def test_contains_activity(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "直近7日の受信: 4件" in text
        assert "OCR未処理: 2件" in text

    def test_contains_warnings_section(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "## 注意事項" in text
        assert "期限切れが3社あります" in text

    def test_no_warnings_section_when_empty(self) -> None:
        report = self._sample_report()
        report.warnings = []
        text = format_briefing_text(report)
        assert "## 注意事項" not in text

    def test_markdown_headers(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "## 全体状況" in text
        assert "## 直近の活動" in text

    def test_unmatched_shown(self) -> None:
        text = format_briefing_text(self._sample_report())
        assert "未紐付けメール: 1件" in text

    def test_no_pipeline_run(self) -> None:
        report = self._sample_report()
        report.last_pipeline_run = None
        text = format_briefing_text(report)
        assert "未実行" in text


# ===========================================================================
# format_briefing_json
# ===========================================================================
class TestFormatBriefingJson:

    def test_valid_json(self) -> None:
        report = BriefingReport(
            generated_at="2026-04-06T10:00:00",
            total_companies=10,
        )
        result = format_briefing_json(report)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_all_fields_present(self) -> None:
        report = BriefingReport(
            generated_at="2026-04-06T10:00:00",
            total_companies=145,
            companies_with_permits=98,
            companies_without_permits=47,
            expired=3,
            warnings=["test warning"],
        )
        parsed = json.loads(format_briefing_json(report))
        assert parsed["total_companies"] == 145
        assert parsed["expired"] == 3
        assert parsed["warnings"] == ["test warning"]

    def test_japanese_not_escaped(self) -> None:
        report = BriefingReport(
            generated_at="2026-04-06T10:00:00",
            warnings=["期限切れが3社あります"],
        )
        result = format_briefing_json(report)
        assert "期限切れ" in result
        assert "\\u" not in result

    def test_null_last_pipeline(self) -> None:
        report = BriefingReport(
            generated_at="2026-04-06T10:00:00",
            last_pipeline_run=None,
        )
        parsed = json.loads(format_briefing_json(report))
        assert parsed["last_pipeline_run"] is None
