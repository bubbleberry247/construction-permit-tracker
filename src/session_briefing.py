"""
session_briefing.py -- セッション開始時のシステム状態サマリー生成

Usage:
    python -X utf8 src/session_briefing.py                  # テキスト出力
    python -X utf8 src/session_briefing.py --json           # JSON出力
    python -X utf8 src/session_briefing.py --db path/to.db  # DB指定
"""
from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class BriefingReport:
    """セッションブリーフィングの構造化レポート"""

    generated_at: str  # ISO 8601
    total_companies: int = 0
    companies_with_permits: int = 0
    companies_without_permits: int = 0
    expiring_90d: int = 0
    expiring_60d: int = 0
    expiring_30d: int = 0
    expired: int = 0
    recent_submissions: int = 0
    pending_ocr: int = 0
    last_pipeline_run: Optional[str] = None
    unmatched_emails: int = 0
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 集計ヘルパー (private)
# ---------------------------------------------------------------------------

def _count_companies(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """(total, with_permits, without_permits) を返す"""
    total = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    with_permits = conn.execute(
        "SELECT COUNT(DISTINCT company_id) FROM permits WHERE current_flag = 1"
    ).fetchone()[0]
    without = total - with_permits
    return total, with_permits, without


def _count_expiring(
    conn: sqlite3.Connection,
    reference_date: Optional[date] = None,
) -> dict[str, int]:
    """期限別の件数を返す: {90d, 60d, 30d, expired}

    reference_date が None の場合は date.today() を使用。
    各バケットは排他的:
      expired  = expiry_date <= ref
      30d      = ref < expiry_date <= ref+30
      60d      = ref+30 < expiry_date <= ref+60
      90d      = ref+60 < expiry_date <= ref+90
    """
    ref = reference_date or date.today()
    ref_iso = ref.isoformat()
    d30 = (ref + timedelta(days=30)).isoformat()
    d60 = (ref + timedelta(days=60)).isoformat()
    d90 = (ref + timedelta(days=90)).isoformat()

    def _cnt(where: str, params: tuple[str, ...]) -> int:
        sql = (
            "SELECT COUNT(*) FROM permits "
            "WHERE current_flag = 1 "
            "AND expiry_date IS NOT NULL "
            "AND expiry_date != 'UNCERTAIN' "
            f"AND {where}"
        )
        return conn.execute(sql, params).fetchone()[0]

    return {
        "expired": _cnt("expiry_date <= ?", (ref_iso,)),
        "30d": _cnt("expiry_date > ? AND expiry_date <= ?", (ref_iso, d30)),
        "60d": _cnt("expiry_date > ? AND expiry_date <= ?", (d30, d60)),
        "90d": _cnt("expiry_date > ? AND expiry_date <= ?", (d60, d90)),
    }


def _count_recent_submissions(
    conn: sqlite3.Connection, days: int = 7
) -> int:
    """直近N日の受信メール件数"""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    return conn.execute(
        "SELECT COUNT(*) FROM inbound_messages WHERE received_at >= ?",
        (cutoff,),
    ).fetchone()[0]


def _count_pending_ocr(conn: sqlite3.Connection) -> int:
    """OCR未処理ファイル数 (pages にあるが ocr_runs に未登録)"""
    return conn.execute(
        "SELECT COUNT(*) FROM pages p "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM ocr_runs o WHERE o.page_id = p.page_id"
        ")"
    ).fetchone()[0]


def _get_last_pipeline_run(conn: sqlite3.Connection) -> Optional[str]:
    """最終パイプライン実行日時 (jobs テーブルの最新完了)"""
    row = conn.execute(
        "SELECT finished_at FROM jobs "
        "WHERE status = 'completed' "
        "ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _count_unmatched(conn: sqlite3.Connection) -> int:
    """company_id が NULL の受信メール数"""
    return conn.execute(
        "SELECT COUNT(*) FROM inbound_messages WHERE company_id IS NULL"
    ).fetchone()[0]


def _check_warnings(
    conn: sqlite3.Connection,
    data_root: Optional[Path] = None,
) -> list[str]:
    """注意事項を検出してリストで返す"""
    warnings: list[str] = []

    # 期限切れ
    expired = conn.execute(
        "SELECT COUNT(*) FROM permits "
        "WHERE current_flag = 1 "
        "AND expiry_date IS NOT NULL "
        "AND expiry_date != 'UNCERTAIN' "
        "AND expiry_date <= date('now', 'localtime')"
    ).fetchone()[0]
    if expired > 0:
        warnings.append(f"期限切れが{expired}社あります")

    # OCR 未処理
    pending = _count_pending_ocr(conn)
    if pending > 0:
        warnings.append(f"OCR未処理ファイルが{pending}件あります")

    # 未マッチメール
    unmatched = _count_unmatched(conn)
    if unmatched > 0:
        warnings.append(f"未紐付けメールが{unmatched}件あります")

    # パイプライン長期未実行 (3日超)
    last_run = _get_last_pipeline_run(conn)
    if last_run:
        try:
            last_dt = datetime.fromisoformat(last_run)
            if datetime.now() - last_dt > timedelta(days=3):
                warnings.append(
                    f"パイプラインが3日以上未実行です（最終: {last_run}）"
                )
        except ValueError:
            pass
    else:
        warnings.append("パイプラインの実行履歴がありません")

    # data_root 存在チェック
    if data_root and not data_root.exists():
        warnings.append(f"データディレクトリが存在しません: {data_root}")

    return warnings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_briefing(
    db_path: Path,
    data_root: Optional[Path] = None,
) -> BriefingReport:
    """DBとファイルシステムからブリーフィングレポートを生成する

    Raises:
        FileNotFoundError: db_path が存在しない場合
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DBファイルが見つかりません: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        total, with_p, without_p = _count_companies(conn)
        expiring = _count_expiring(conn)
        recent = _count_recent_submissions(conn)
        pending = _count_pending_ocr(conn)
        last_run = _get_last_pipeline_run(conn)
        unmatched = _count_unmatched(conn)
        warnings = _check_warnings(conn, data_root)

        return BriefingReport(
            generated_at=datetime.now().isoformat(timespec="seconds"),
            total_companies=total,
            companies_with_permits=with_p,
            companies_without_permits=without_p,
            expiring_90d=expiring["90d"],
            expiring_60d=expiring["60d"],
            expiring_30d=expiring["30d"],
            expired=expiring["expired"],
            recent_submissions=recent,
            pending_ocr=pending,
            last_pipeline_run=last_run,
            unmatched_emails=unmatched,
            warnings=warnings,
        )
    finally:
        conn.close()


def format_briefing_text(report: BriefingReport) -> str:
    """人間可読なマークダウン形式のブリーフィングを生成"""
    gen_date = report.generated_at[:10]
    lines = [
        f"# Session Briefing ({gen_date})",
        "",
        "## 全体状況",
        (
            f"- 登録会社: {report.total_companies}社"
            f"（許可証あり: {report.companies_with_permits}社"
            f" / なし: {report.companies_without_permits}社）"
        ),
        (
            f"- 期限切れ: {report.expired}社"
            f" | 30日以内: {report.expiring_30d}社"
            f" | 60日以内: {report.expiring_60d}社"
            f" | 90日以内: {report.expiring_90d}社"
        ),
        "",
        "## 直近の活動",
        f"- 直近7日の受信: {report.recent_submissions}件",
        f"- OCR未処理: {report.pending_ocr}件",
        f"- 最終パイプライン実行: {report.last_pipeline_run or '未実行'}",
        f"- 未紐付けメール: {report.unmatched_emails}件",
    ]

    if report.warnings:
        lines.append("")
        lines.append("## 注意事項")
        for w in report.warnings:
            lines.append(f"- \u26a0 {w}")

    lines.append("")
    return "\n".join(lines)


def format_briefing_json(report: BriefingReport) -> str:
    """JSON形式のブリーフィングを出力"""
    return json.dumps(asdict(report), ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="セッションブリーフィング生成"
    )
    parser.add_argument(
        "--db", type=Path, default=DB_PATH, help="DBファイルパス"
    )
    parser.add_argument(
        "--json", action="store_true", dest="as_json", help="JSON出力"
    )
    parser.add_argument(
        "--data-root", type=Path, default=None, help="データルート"
    )
    args = parser.parse_args()

    report = generate_briefing(args.db, args.data_root)

    if args.as_json:
        print(format_briefing_json(report))
    else:
        print(format_briefing_text(report))


if __name__ == "__main__":
    main()
