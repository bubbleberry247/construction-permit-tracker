"""
staging/ 内の建設業許可証 PDF を OCR 処理し、staging_permits_YYYYMMDD_HHMMSS.csv を生成する。
ocr_permit.py の process_pdf() を再利用（ファイル移動はしない）。
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from ocr_permit import (  # noqa: E402
    process_pdf,
    load_config,
    write_staging_csv,
    _build_sender_email_lookup,
    logger,
)

STAGING_DIR = PROJECT_ROOT / "data" / "staging"
OUTPUT_DIR = PROJECT_ROOT / "output"

PERMIT_KEYWORDS = ["建設業許可", "建築業許可", "許可証", "許可通知"]


def is_permit_candidate(name: str) -> bool:
    return any(k in name for k in PERMIT_KEYWORDS)


def collect_permit_pdfs() -> list[Path]:
    files: list[Path] = []
    for d in sorted(STAGING_DIR.iterdir()):
        if not d.is_dir():
            continue
        for f in sorted(d.iterdir()):
            if f.is_file() and f.suffix.lower() == ".pdf" and is_permit_candidate(f.name):
                files.append(f)
    return files


def main() -> None:
    config = load_config()
    data_root = Path(config["DATA_ROOT"])
    sender_lookup = _build_sender_email_lookup(data_root)

    pdfs = collect_permit_pdfs()
    logger.info("OCR対象: %d 件", len(pdfs))
    for p in pdfs:
        logger.info("  - %s", p.relative_to(PROJECT_ROOT))

    records = []
    known_hashes: set[str] = set()
    for i, p in enumerate(pdfs, 1):
        logger.info("[%d/%d] %s", i, len(pdfs), p.name)
        sender_email = sender_lookup.get(p.name, "")
        try:
            rec = process_pdf(p, config, known_hashes, sender_email=sender_email)
        except Exception as exc:  # noqa: BLE001
            logger.error("ERROR [%s]: %s", p.name, exc)
            from ocr_permit import STAGING_CSV_COLUMNS, TIER1_VALIDATION_ERROR
            rec = {col: "" for col in STAGING_CSV_COLUMNS}
            rec["source_file"] = p.name
            rec["processed_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            rec["parse_status"] = "ERROR"
            rec["error_category"] = TIER1_VALIDATION_ERROR
            rec["error_reason"] = str(exc)
        logger.info("  → parse_status=%s", rec.get("parse_status"))
        records.append(rec)

    if records:
        csv_path = write_staging_csv(records, OUTPUT_DIR)
        logger.info("CSV出力: %s", csv_path)
    logger.info("完了: %d 件処理", len(records))


if __name__ == "__main__":
    main()
