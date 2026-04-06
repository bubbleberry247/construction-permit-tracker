"""
evidence_ledger.py — パイプライン全操作の監査証跡台帳

保存形式: JSONL（1行1エントリ）
デフォルトパス: data/logs/evidence_ledger.jsonl

Usage:
    from evidence_ledger import EvidenceLedger, EvidenceEntry
    ledger = EvidenceLedger(Path("data/logs/evidence_ledger.jsonl"))
    ledger.record(EvidenceEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        operation="ocr_extract",
        actor="ocr_permit.py",
        target="C001",
        input_summary="permit_scan_001.pdf (2 pages)",
        output_summary="permit_number=愛知県知事 許可（特-6）第57805号",
        decision="matched",
        confidence=0.8,
    ))
"""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

CSV_COLUMNS: list[str] = [
    "timestamp", "operation", "actor", "target",
    "input_summary", "output_summary", "decision",
    "confidence", "details",
]


@dataclass
class EvidenceEntry:
    """監査証跡の1エントリ。"""

    timestamp: str  # ISO 8601
    operation: str  # "ocr_extract", "reconcile", "sheets_upsert", "mlit_verify", etc.
    actor: str  # "pipeline", "manual", "ocr_permit.py", etc.
    target: str  # company_id or file path or permit_id
    input_summary: str
    output_summary: str
    decision: str  # "matched", "unmatched", "skipped", "error", etc.
    confidence: float | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """シリアライズ用の辞書を返す。"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EvidenceEntry:
        """辞書からインスタンスを復元する。"""
        return cls(
            timestamp=str(data.get("timestamp", "")),
            operation=str(data.get("operation", "")),
            actor=str(data.get("actor", "")),
            target=str(data.get("target", "")),
            input_summary=str(data.get("input_summary", "")),
            output_summary=str(data.get("output_summary", "")),
            decision=str(data.get("decision", "")),
            confidence=data.get("confidence"),
            details=data.get("details") or {},
        )


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------


class EvidenceLedger:
    """JSONLファイルベースの証跡台帳。"""

    def __init__(self, ledger_path: Path) -> None:
        self._path = ledger_path

    @property
    def path(self) -> Path:
        return self._path

    # ---- Write ----

    def record(self, entry: EvidenceEntry) -> None:
        """証跡エントリを追記（append-only）。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")

    # ---- Read ----

    def _load_all(self) -> list[EvidenceEntry]:
        """全エントリを読み込む。不正行はスキップしてログ出力。"""
        if not self._path.exists():
            return []

        entries: list[EvidenceEntry] = []
        with self._path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    data = json.loads(stripped)
                    entries.append(EvidenceEntry.from_dict(data))
                except (json.JSONDecodeError, TypeError) as exc:
                    logger.warning(
                        "証跡台帳 行%d をスキップ（JSON不正）: %s", line_no, exc,
                    )
        return entries

    def query(
        self,
        operation: str | None = None,
        target: str | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[EvidenceEntry]:
        """条件に合致するエントリを検索。"""
        entries = self._load_all()
        result: list[EvidenceEntry] = []

        for entry in entries:
            if operation and entry.operation != operation:
                continue
            if target and entry.target != target:
                continue
            if since and entry.timestamp < since:
                continue
            if until and entry.timestamp > until:
                continue
            result.append(entry)

        return result

    def summary(self) -> dict[str, Any]:
        """操作種別ごとの件数・最終実行日時サマリー。"""
        entries = self._load_all()
        ops: dict[str, dict[str, Any]] = {}

        for entry in entries:
            op = entry.operation
            if op not in ops:
                ops[op] = {"count": 0, "last_timestamp": ""}
            ops[op]["count"] += 1
            if entry.timestamp > ops[op]["last_timestamp"]:
                ops[op]["last_timestamp"] = entry.timestamp

        return {
            "total_entries": len(entries),
            "operations": ops,
        }

    def export_csv(self, output_path: Path) -> int:
        """CSV形式（BOM付きUTF-8）でエクスポート。書き出し行数を返す。"""
        entries = self._load_all()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for entry in entries:
                row = entry.to_dict()
                # details を JSON 文字列化
                row["details"] = json.dumps(
                    row["details"], ensure_ascii=False,
                )
                writer.writerow(row)

        return len(entries)
