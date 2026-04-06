"""
tests/test_rpa_drift_watchdog.py — ドリフト検知ウォッチドッグ ユニットテスト

テスト対象:
  - src/rpa_drift_watchdog.py

実行:
    cd <project_root>
    pytest tests/test_rpa_drift_watchdog.py -v
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

from rpa_drift_watchdog import (  # noqa: E402
    DriftAlert,
    DriftWatchdog,
    MetricSnapshot,
    calculate_error_rate,
    calculate_match_rate,
    calculate_ocr_success_rate,
    calculate_pipeline_duration,
)


# ===========================================================================
# Fixtures
# ===========================================================================
@pytest.fixture()
def test_db(tmp_path: Path) -> Path:
    """テスト用SQLiteDBを作成して返す"""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE ocr_runs (
            run_id INTEGER PRIMARY KEY,
            page_id INTEGER,
            run_type TEXT,
            model_name TEXT,
            status TEXT,
            created_at TEXT
        );
        CREATE TABLE inbound_messages (
            message_id TEXT PRIMARY KEY,
            company_id TEXT,
            received_at TEXT
        );
        CREATE TABLE jobs (
            job_id INTEGER PRIMARY KEY,
            job_type TEXT,
            status TEXT,
            started_at TEXT,
            finished_at TEXT,
            result_summary TEXT,
            created_at TEXT
        );
        CREATE TABLE files (
            file_id INTEGER PRIMARY KEY,
            message_id TEXT,
            company_id TEXT
        );
    """)
    conn.close()
    return db_path


@pytest.fixture()
def populated_db(test_db: Path) -> Path:
    """テストデータ入りDB"""
    today = date.today().isoformat()
    conn = sqlite3.connect(str(test_db))
    # OCR runs: 8 completed, 2 error
    for i in range(1, 11):
        status = "completed" if i <= 8 else "error"
        conn.execute(
            "INSERT INTO ocr_runs (run_id, page_id, run_type, model_name, status, created_at) "
            "VALUES (?, ?, 'full', 'gpt-4o', ?, ?)",
            (i, i, status, today),
        )
    # Inbound messages: 7 matched, 3 unmatched
    for i in range(1, 11):
        cid = f"C{i:03d}" if i <= 7 else ""
        conn.execute(
            "INSERT INTO inbound_messages (message_id, company_id, received_at) "
            "VALUES (?, ?, ?)",
            (f"msg_{i}", cid, today),
        )
    # Jobs: 9 completed, 1 failed
    for i in range(1, 11):
        status = "completed" if i <= 9 else "failed"
        conn.execute(
            "INSERT INTO jobs (job_id, job_type, status, started_at, finished_at, created_at) "
            "VALUES (?, 'ocr', ?, ?, ?, ?)",
            (
                i,
                status,
                f"{today} 07:00:00",
                f"{today} 07:05:00",
                today,
            ),
        )
    conn.commit()
    conn.close()
    return test_db


@pytest.fixture()
def metrics_file(tmp_path: Path) -> Path:
    """メトリクスJSONLファイルパスを返す（ファイルは未作成）"""
    return tmp_path / "drift_metrics.jsonl"


@pytest.fixture()
def watchdog(metrics_file: Path) -> DriftWatchdog:
    """デフォルト閾値の DriftWatchdog"""
    return DriftWatchdog(metrics_path=metrics_file)


# ===========================================================================
# calculate_ocr_success_rate
# ===========================================================================
class TestCalculateOcrSuccessRate:

    def test_returns_correct_rate(self, populated_db: Path) -> None:
        conn = sqlite3.connect(str(populated_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        rate = calculate_ocr_success_rate(conn, since)
        conn.close()
        assert rate == 80.0  # 8/10

    def test_empty_db_returns_100(self, test_db: Path) -> None:
        conn = sqlite3.connect(str(test_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        rate = calculate_ocr_success_rate(conn, since)
        conn.close()
        assert rate == 100.0


# ===========================================================================
# calculate_match_rate
# ===========================================================================
class TestCalculateMatchRate:

    def test_returns_correct_rate(self, populated_db: Path) -> None:
        conn = sqlite3.connect(str(populated_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        rate = calculate_match_rate(conn, since)
        conn.close()
        assert rate == 70.0  # 7/10

    def test_empty_db_returns_100(self, test_db: Path) -> None:
        conn = sqlite3.connect(str(test_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        rate = calculate_match_rate(conn, since)
        conn.close()
        assert rate == 100.0


# ===========================================================================
# calculate_error_rate
# ===========================================================================
class TestCalculateErrorRate:

    def test_returns_correct_rate(self, populated_db: Path) -> None:
        conn = sqlite3.connect(str(populated_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        rate = calculate_error_rate(conn, since)
        conn.close()
        assert rate == 10.0  # 1/10

    def test_empty_db_returns_zero(self, test_db: Path) -> None:
        conn = sqlite3.connect(str(test_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        rate = calculate_error_rate(conn, since)
        conn.close()
        assert rate == 0.0


# ===========================================================================
# calculate_pipeline_duration
# ===========================================================================
class TestCalculatePipelineDuration:

    def test_returns_duration_seconds(self, populated_db: Path) -> None:
        conn = sqlite3.connect(str(populated_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        dur = calculate_pipeline_duration(conn, since)
        conn.close()
        assert dur == 300.0  # 5分 = 300秒

    def test_empty_db_returns_none(self, test_db: Path) -> None:
        conn = sqlite3.connect(str(test_db))
        since = (date.today() - timedelta(days=1)).isoformat()
        dur = calculate_pipeline_duration(conn, since)
        conn.close()
        assert dur is None


# ===========================================================================
# DriftWatchdog.collect_metrics
# ===========================================================================
class TestCollectMetrics:

    def test_collects_from_populated_db(
        self, watchdog: DriftWatchdog, populated_db: Path
    ) -> None:
        snapshots = watchdog.collect_metrics(populated_db)
        names = [s.metric_name for s in snapshots]
        assert "ocr_success_rate" in names
        assert "match_rate" in names
        assert "error_rate" in names
        assert "pipeline_duration_sec" in names

    def test_collects_correct_values(
        self, watchdog: DriftWatchdog, populated_db: Path
    ) -> None:
        snapshots = watchdog.collect_metrics(populated_db)
        by_name = {s.metric_name: s.value for s in snapshots}
        assert by_name["ocr_success_rate"] == 80.0
        assert by_name["match_rate"] == 70.0
        assert by_name["error_rate"] == 10.0
        assert by_name["pipeline_duration_sec"] == 300.0

    def test_collects_from_empty_db(
        self, watchdog: DriftWatchdog, test_db: Path
    ) -> None:
        """空DBでエラーにならない"""
        snapshots = watchdog.collect_metrics(test_db)
        assert len(snapshots) >= 2  # ocr, match, error (duration is None → skip)
        # pipeline_duration_sec は None なのでスナップショットに含まれない
        names = [s.metric_name for s in snapshots]
        assert "pipeline_duration_sec" not in names


# ===========================================================================
# DriftWatchdog.record_metrics
# ===========================================================================
class TestRecordMetrics:

    def test_creates_file_and_writes_jsonl(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        snapshots = [
            MetricSnapshot("2026-04-06", "ocr_success_rate", 95.0),
            MetricSnapshot("2026-04-06", "match_rate", 85.0),
        ]
        watchdog.record_metrics(snapshots)

        assert metrics_file.exists()
        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        rec = json.loads(lines[0])
        assert rec["metric_name"] == "ocr_success_rate"
        assert rec["value"] == 95.0

    def test_appends_to_existing_file(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """既存ファイルへの追記"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        metrics_file.write_text(
            json.dumps({"date": "2026-04-05", "metric_name": "ocr_success_rate",
                         "value": 90.0, "context": {}}) + "\n",
            encoding="utf-8",
        )
        watchdog.record_metrics([
            MetricSnapshot("2026-04-06", "ocr_success_rate", 95.0),
        ])
        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """親ディレクトリが存在しなくても自動作成"""
        deep_path = tmp_path / "a" / "b" / "c" / "metrics.jsonl"
        wd = DriftWatchdog(metrics_path=deep_path)
        wd.record_metrics([
            MetricSnapshot("2026-04-06", "error_rate", 5.0),
        ])
        assert deep_path.exists()


# ===========================================================================
# DriftWatchdog.get_baseline
# ===========================================================================
class TestGetBaseline:

    def test_returns_average(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """十分なデータがある場合の平均値"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        today = date.today()
        lines = []
        for i in range(10):
            d = (today - timedelta(days=i)).isoformat()
            lines.append(json.dumps({
                "date": d, "metric_name": "ocr_success_rate",
                "value": 90.0 + i, "context": {},
            }))
        metrics_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        baseline = watchdog.get_baseline("ocr_success_rate", window_days=30)
        assert baseline is not None
        # 平均 = (90+91+...+99)/10 = 94.5
        assert baseline == 94.5

    def test_returns_none_when_no_file(
        self, watchdog: DriftWatchdog
    ) -> None:
        """メトリクスファイルがない場合 None"""
        assert watchdog.get_baseline("ocr_success_rate") is None

    def test_returns_none_when_no_matching_metric(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """該当メトリクスがない場合 None"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        metrics_file.write_text(
            json.dumps({"date": date.today().isoformat(),
                         "metric_name": "other_metric",
                         "value": 50.0, "context": {}}) + "\n",
            encoding="utf-8",
        )
        assert watchdog.get_baseline("ocr_success_rate") is None

    def test_excludes_old_data(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """window外のデータは除外"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        old_date = (date.today() - timedelta(days=60)).isoformat()
        recent_date = date.today().isoformat()
        lines = [
            json.dumps({"date": old_date, "metric_name": "ocr_success_rate",
                         "value": 50.0, "context": {}}),
            json.dumps({"date": recent_date, "metric_name": "ocr_success_rate",
                         "value": 90.0, "context": {}}),
        ]
        metrics_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        baseline = watchdog.get_baseline("ocr_success_rate", window_days=30)
        assert baseline == 90.0  # 古いデータは除外


# ===========================================================================
# DriftWatchdog.detect_drift
# ===========================================================================
class TestDetectDrift:

    def _seed_baseline(
        self, metrics_file: Path, metric_name: str, value: float, days: int = 10
    ) -> None:
        """ベースラインデータをJSONLに書き込む"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        for i in range(days):
            d = (date.today() - timedelta(days=i + 1)).isoformat()
            lines.append(json.dumps({
                "date": d, "metric_name": metric_name,
                "value": value, "context": {},
            }))
        with metrics_file.open("a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    def test_no_alert_within_threshold(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """閾値内 → 空リスト"""
        self._seed_baseline(metrics_file, "ocr_success_rate", 95.0)
        current = [MetricSnapshot(date.today().isoformat(), "ocr_success_rate", 93.0)]
        alerts = watchdog.detect_drift(current)
        assert alerts == []

    def test_alert_on_ocr_rate_drop(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """OCR成功率10%以上低下 → warning"""
        self._seed_baseline(metrics_file, "ocr_success_rate", 95.0)
        # 95 → 80 = -15.79%
        current = [MetricSnapshot(date.today().isoformat(), "ocr_success_rate", 80.0)]
        alerts = watchdog.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].severity == "warning"
        assert alerts[0].metric_name == "ocr_success_rate"

    def test_critical_on_ocr_rate_severe_drop(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """OCR成功率20%以上低下 → critical"""
        self._seed_baseline(metrics_file, "ocr_success_rate", 100.0)
        # 100 → 75 = -25%
        current = [MetricSnapshot(date.today().isoformat(), "ocr_success_rate", 75.0)]
        alerts = watchdog.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].severity == "critical"

    def test_alert_on_error_rate_increase(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """エラー率20%以上増加 → warning"""
        self._seed_baseline(metrics_file, "error_rate", 5.0)
        # 5 → 7 = +40%
        current = [MetricSnapshot(date.today().isoformat(), "error_rate", 7.0)]
        alerts = watchdog.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].severity == "warning"

    def test_critical_on_error_rate_spike(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """エラー率50%以上増加 → critical"""
        self._seed_baseline(metrics_file, "error_rate", 10.0)
        # 10 → 16 = +60%
        current = [MetricSnapshot(date.today().isoformat(), "error_rate", 16.0)]
        alerts = watchdog.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].severity == "critical"

    def test_no_alert_when_no_baseline(
        self, watchdog: DriftWatchdog
    ) -> None:
        """ベースラインなし → アラートなし"""
        current = [MetricSnapshot(date.today().isoformat(), "ocr_success_rate", 50.0)]
        alerts = watchdog.detect_drift(current)
        assert alerts == []

    def test_no_alert_on_rate_increase(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """OCR成功率が向上した場合 → アラートなし"""
        self._seed_baseline(metrics_file, "ocr_success_rate", 80.0)
        current = [MetricSnapshot(date.today().isoformat(), "ocr_success_rate", 95.0)]
        alerts = watchdog.detect_drift(current)
        assert alerts == []

    def test_match_rate_warning(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """マッチ率15%以上低下 → warning"""
        self._seed_baseline(metrics_file, "match_rate", 90.0)
        # 90 → 72 = -20%
        current = [MetricSnapshot(date.today().isoformat(), "match_rate", 72.0)]
        alerts = watchdog.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].severity == "warning"


# ===========================================================================
# DriftWatchdog.run (integration)
# ===========================================================================
class TestRunIntegration:

    def test_run_with_populated_db(
        self, watchdog: DriftWatchdog, populated_db: Path, metrics_file: Path
    ) -> None:
        """run() が一連フローを正しく実行する"""
        alerts = watchdog.run(populated_db)
        # 初回実行 → ベースラインなし → アラートなし
        assert alerts == []
        # ファイルが作成されていること
        assert metrics_file.exists()
        lines = metrics_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) >= 3  # ocr, match, error (+ duration)

    def test_run_with_empty_db(
        self, watchdog: DriftWatchdog, test_db: Path, metrics_file: Path
    ) -> None:
        """空DBでエラーにならない"""
        alerts = watchdog.run(test_db)
        assert isinstance(alerts, list)
        assert metrics_file.exists()

    def test_run_detects_drift_on_second_run(
        self, metrics_file: Path, populated_db: Path
    ) -> None:
        """2回目の実行でドリフトが検知される（ベースラインを人為的にずらす）"""
        wd = DriftWatchdog(metrics_path=metrics_file)

        # ベースラインを高い値で仕込む（過去30日分）
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        for i in range(30):
            d = (date.today() - timedelta(days=i + 1)).isoformat()
            for rec in [
                {"date": d, "metric_name": "ocr_success_rate",
                 "value": 100.0, "context": {}},
                {"date": d, "metric_name": "match_rate",
                 "value": 100.0, "context": {}},
                {"date": d, "metric_name": "error_rate",
                 "value": 0.0, "context": {}},
            ]:
                with metrics_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(rec) + "\n")

        # run: 実際のDB値 (ocr=80%, match=70%) はベースラインから大幅に乖離
        alerts = wd.run(populated_db)
        alert_names = [a.metric_name for a in alerts]
        assert "ocr_success_rate" in alert_names
        assert "match_rate" in alert_names


# ===========================================================================
# Edge cases
# ===========================================================================
class TestEdgeCases:

    def test_metrics_file_auto_created(self, tmp_path: Path) -> None:
        """メトリクスファイルが存在しない場合に自動作成"""
        path = tmp_path / "new_dir" / "metrics.jsonl"
        wd = DriftWatchdog(metrics_path=path)
        wd.record_metrics([
            MetricSnapshot("2026-04-06", "ocr_success_rate", 95.0),
        ])
        assert path.exists()

    def test_custom_thresholds(self, tmp_path: Path) -> None:
        """カスタム閾値が適用される"""
        path = tmp_path / "m.jsonl"
        wd = DriftWatchdog(
            metrics_path=path,
            thresholds={"custom_metric": 5.0},
        )
        # ベースラインを仕込む
        path.parent.mkdir(parents=True, exist_ok=True)
        for i in range(10):
            d = (date.today() - timedelta(days=i + 1)).isoformat()
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "date": d, "metric_name": "custom_metric",
                    "value": 100.0, "context": {},
                }) + "\n")

        # 5%超乖離でアラート
        current = [MetricSnapshot(date.today().isoformat(), "custom_metric", 108.0)]
        alerts = wd.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].severity == "warning"

    def test_corrupt_jsonl_line_skipped(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """壊れたJSONL行はスキップされる"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "this is not json",
            json.dumps({"date": date.today().isoformat(),
                         "metric_name": "ocr_success_rate",
                         "value": 90.0, "context": {}}),
        ]
        metrics_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        baseline = watchdog.get_baseline("ocr_success_rate")
        assert baseline == 90.0

    def test_deviation_pct_in_alert(
        self, watchdog: DriftWatchdog, metrics_file: Path
    ) -> None:
        """DriftAlert.deviation_pct が正しい値を持つ"""
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        for i in range(10):
            d = (date.today() - timedelta(days=i + 1)).isoformat()
            with metrics_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "date": d, "metric_name": "ocr_success_rate",
                    "value": 100.0, "context": {},
                }) + "\n")

        current = [MetricSnapshot(date.today().isoformat(), "ocr_success_rate", 75.0)]
        alerts = watchdog.detect_drift(current)
        assert len(alerts) == 1
        assert alerts[0].deviation_pct == -25.0
        assert alerts[0].baseline_value == 100.0
        assert alerts[0].current_value == 75.0
