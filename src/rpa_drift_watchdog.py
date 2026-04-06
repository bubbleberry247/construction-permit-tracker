"""
rpa_drift_watchdog.py
パイプライン品質ドリフト検知ウォッチドッグ

パイプライン (fetch_gmail → ocr_permit → register_sheets) の品質指標を
JSONL 形式で追跡し、ベースラインからの乖離を検知する。

Usage:
    python src/rpa_drift_watchdog.py                 # デフォルト実行
    python src/rpa_drift_watchdog.py --db data/permit_tracker.db
    python src/rpa_drift_watchdog.py --metrics data/logs/drift_metrics.jsonl
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).parent.parent
_DEFAULT_DB = _PROJECT_ROOT / "data" / "permit_tracker.db"
_DEFAULT_METRICS = _PROJECT_ROOT / "data" / "logs" / "drift_metrics.jsonl"

# ---------------------------------------------------------------------------
# Default thresholds (metric_name → deviation %)
# ---------------------------------------------------------------------------
DEFAULT_THRESHOLDS: dict[str, float] = {
    "ocr_success_rate": 10.0,
    "match_rate": 15.0,
    "error_rate": 20.0,
    "pipeline_duration_sec": 50.0,
}

# Severity escalation: metric_name → list of (threshold_pct, severity)
# 評価は大きい順 (critical first) で行う
SEVERITY_RULES: dict[str, list[tuple[float, str]]] = {
    "ocr_success_rate": [(20.0, "critical"), (10.0, "warning")],
    "match_rate": [(30.0, "critical"), (15.0, "warning")],
    "error_rate": [(50.0, "critical"), (20.0, "warning")],
    "pipeline_duration_sec": [(100.0, "critical"), (50.0, "warning")],
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class MetricSnapshot:
    date: str  # YYYY-MM-DD
    metric_name: str
    value: float
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class DriftAlert:
    metric_name: str
    current_value: float
    baseline_value: float
    deviation_pct: float  # 乖離率（%）
    severity: str  # "info", "warning", "critical"
    message: str


# ---------------------------------------------------------------------------
# Metric collectors (standalone functions)
# ---------------------------------------------------------------------------
def calculate_ocr_success_rate(conn: sqlite3.Connection, since: str) -> float:
    """OCR成功率 = status='completed' / total * 100"""
    row = conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS ok "
        "FROM ocr_runs WHERE created_at >= ?",
        (since,),
    ).fetchone()
    total = row[0] if row else 0
    if total == 0:
        return 100.0  # データなし = 異常なしとみなす
    return round(row[1] / total * 100, 2)


def calculate_match_rate(conn: sqlite3.Connection, since: str) -> float:
    """マッチ率 = company_id IS NOT NULL AND != '' / total * 100"""
    row = conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN company_id IS NOT NULL AND company_id != '' "
        "THEN 1 ELSE 0 END) AS matched "
        "FROM inbound_messages WHERE received_at >= ?",
        (since,),
    ).fetchone()
    total = row[0] if row else 0
    if total == 0:
        return 100.0
    return round(row[1] / total * 100, 2)


def calculate_error_rate(conn: sqlite3.Connection, since: str) -> float:
    """エラー率 = status in ('error','failed') / total * 100"""
    row = conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status IN ('error', 'failed') THEN 1 ELSE 0 END) AS errs "
        "FROM jobs WHERE started_at >= ?",
        (since,),
    ).fetchone()
    total = row[0] if row else 0
    if total == 0:
        return 0.0  # ジョブなし = エラーなし
    return round(row[1] / total * 100, 2)


def calculate_pipeline_duration(
    conn: sqlite3.Connection, since: str
) -> float | None:
    """最新パイプライン実行時間（秒）。ジョブがなければ None。"""
    row = conn.execute(
        "SELECT started_at, finished_at FROM jobs "
        "WHERE started_at >= ? AND finished_at IS NOT NULL "
        "ORDER BY started_at DESC LIMIT 1",
        (since,),
    ).fetchone()
    if not row or not row[0] or not row[1]:
        return None
    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        start = datetime.strptime(row[0][:19], fmt)
        end = datetime.strptime(row[1][:19], fmt)
        return round((end - start).total_seconds(), 2)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# DriftWatchdog class
# ---------------------------------------------------------------------------
class DriftWatchdog:
    """パイプライン品質ドリフト検知ウォッチドッグ"""

    def __init__(
        self,
        metrics_path: Path,
        thresholds: dict[str, float] | None = None,
    ) -> None:
        self.metrics_path = metrics_path
        self.thresholds = thresholds or DEFAULT_THRESHOLDS.copy()

    # --- collect ---
    def collect_metrics(
        self, db_path: Path, window_days: int = 1
    ) -> list[MetricSnapshot]:
        """DBから直近N日のメトリクスを収集"""
        since = (date.today() - timedelta(days=window_days)).isoformat()
        today_str = date.today().isoformat()
        snapshots: list[MetricSnapshot] = []

        conn = sqlite3.connect(str(db_path))
        try:
            ocr_rate = calculate_ocr_success_rate(conn, since)
            snapshots.append(
                MetricSnapshot(today_str, "ocr_success_rate", ocr_rate)
            )

            match_rate = calculate_match_rate(conn, since)
            snapshots.append(
                MetricSnapshot(today_str, "match_rate", match_rate)
            )

            err_rate = calculate_error_rate(conn, since)
            snapshots.append(
                MetricSnapshot(today_str, "error_rate", err_rate)
            )

            duration = calculate_pipeline_duration(conn, since)
            if duration is not None:
                snapshots.append(
                    MetricSnapshot(
                        today_str,
                        "pipeline_duration_sec",
                        duration,
                    )
                )
        finally:
            conn.close()

        return snapshots

    # --- record ---
    def record_metrics(self, snapshots: list[MetricSnapshot]) -> None:
        """メトリクスをJSONLファイルに追記"""
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with self.metrics_path.open("a", encoding="utf-8") as f:
            for s in snapshots:
                line = json.dumps(
                    {
                        "date": s.date,
                        "metric_name": s.metric_name,
                        "value": s.value,
                        "context": s.context,
                    },
                    ensure_ascii=False,
                )
                f.write(line + "\n")

    # --- baseline ---
    def get_baseline(
        self, metric_name: str, window_days: int = 30
    ) -> float | None:
        """直近N日の平均値をベースラインとして返す"""
        if not self.metrics_path.exists():
            return None

        cutoff = (date.today() - timedelta(days=window_days)).isoformat()
        values: list[float] = []

        with self.metrics_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("metric_name") != metric_name:
                    continue
                if rec.get("date", "") >= cutoff:
                    values.append(float(rec["value"]))

        if not values:
            return None
        return round(sum(values) / len(values), 4)

    # --- detect ---
    def detect_drift(
        self, current: list[MetricSnapshot]
    ) -> list[DriftAlert]:
        """現在値とベースラインを比較し、閾値を超えたものをアラートとして返す"""
        alerts: list[DriftAlert] = []

        for snap in current:
            baseline = self.get_baseline(snap.metric_name)
            if baseline is None:
                continue
            if baseline == 0:
                # ベースラインが0の場合、乖離率計算不可
                continue

            deviation_pct = ((snap.value - baseline) / abs(baseline)) * 100
            deviation_pct = round(deviation_pct, 2)

            severity = self._classify_severity(
                snap.metric_name, deviation_pct
            )
            if severity is None:
                continue

            direction = "低下" if deviation_pct < 0 else "増加"
            alerts.append(
                DriftAlert(
                    metric_name=snap.metric_name,
                    current_value=snap.value,
                    baseline_value=baseline,
                    deviation_pct=deviation_pct,
                    severity=severity,
                    message=(
                        f"{snap.metric_name}: "
                        f"{baseline:.1f}→{snap.value:.1f} "
                        f"({deviation_pct:+.1f}% {direction})"
                    ),
                )
            )

        return alerts

    def _classify_severity(
        self, metric_name: str, deviation_pct: float
    ) -> str | None:
        """乖離率から severity を判定。閾値内なら None。"""
        rules = SEVERITY_RULES.get(metric_name)
        if rules is None:
            # フォールバック: デフォルト閾値で warning
            threshold = self.thresholds.get(metric_name)
            if threshold is None:
                return None
            if abs(deviation_pct) >= threshold:
                return "warning"
            return None

        # 成功率系メトリクスは低下（負の方向）を検知
        # エラー率・所要時間は増加（正の方向）を検知
        if metric_name in ("ocr_success_rate", "match_rate"):
            check_val = abs(deviation_pct) if deviation_pct < 0 else 0
        else:
            check_val = deviation_pct if deviation_pct > 0 else 0

        for threshold, severity in rules:
            if check_val >= threshold:
                return severity
        return None

    # --- run ---
    def run(self, db_path: Path) -> list[DriftAlert]:
        """collect → record → detect の一連フローを実行"""
        snapshots = self.collect_metrics(db_path)
        logger.info("収集メトリクス: %d 件", len(snapshots))

        self.record_metrics(snapshots)
        logger.info("メトリクス記録完了: %s", self.metrics_path)

        alerts = self.detect_drift(snapshots)
        for a in alerts:
            log_fn = logger.warning if a.severity == "warning" else logger.critical
            log_fn("[%s] %s", a.severity.upper(), a.message)

        return alerts


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="パイプライン品質ドリフト検知"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=_DEFAULT_DB,
        help="SQLite DBパス",
    )
    parser.add_argument(
        "--metrics",
        type=Path,
        default=_DEFAULT_METRICS,
        help="メトリクスJSONLファイルパス",
    )
    args = parser.parse_args()

    watchdog = DriftWatchdog(metrics_path=args.metrics)
    alerts = watchdog.run(db_path=args.db)

    if alerts:
        print(f"\n⚠ ドリフト検知: {len(alerts)} 件")
        for a in alerts:
            print(f"  [{a.severity}] {a.message}")
        sys.exit(1)
    else:
        print("\n✓ ドリフトなし")
        sys.exit(0)


if __name__ == "__main__":
    main()
