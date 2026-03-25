"""
generate_checklist.py — 必要書類チェックリスト生成

staging CSV から会社別に提出書類の有無をチェックリストとして出力する。

Usage:
    python src/generate_checklist.py                           # 最新の staging CSV
    python src/generate_checklist.py output/staging_xxx.csv    # 指定CSV
    python src/generate_checklist.py --all                     # output/ 内の全staging CSVを統合
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).parent.parent

# Required document types (same as DOCUMENT_CHECKLIST_HEADERS in register_sheets.py)
REQUIRED_DOCUMENTS = [
    "新規継続取引申請書",
    "建設業許可証",
    "決算書",  # 前年度+前々年度を統合
    "会社案内",
    "工事経歴書",
    "取引先一覧表",
    "労働安全衛生誓約書",
]

# document_type from GPT-4o → checklist category mapping
DOC_TYPE_MAP = {
    "建設業許可証": "建設業許可証",
    "決算書": "決算書",
    "会社案内": "会社案内",
    "工事経歴書": "工事経歴書",
    "取引先一覧表": "取引先一覧表",
    "新規継続取引申請書": "新規継続取引申請書",
    "労働安全衛生誓約書": "労働安全衛生誓約書",
}


def load_config() -> dict[str, Any]:
    config_path = _PROJECT_ROOT / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


def _normalize_company_name(name: str) -> str:
    if not name:
        return ""
    normalized = unicodedata.normalize("NFKC", name)
    normalized = normalized.replace("\u3000", " ")
    normalized = normalized.replace("㈱", "株式会社")
    normalized = normalized.replace("㈲", "有限会社")
    normalized = re.sub(r"\s*[（(]株[）)]\s*", "株式会社", normalized)
    normalized = re.sub(r"\s*[（(]有[）)]\s*", "有限会社", normalized)
    return normalized.strip()


def _extract_doc_type(row: dict[str, str]) -> str | None:
    """Parse document_type from either parse_status or error_reason."""
    status = row.get("parse_status", "")

    # Permit documents
    if status in ("OK", "REVIEW_NEEDED"):
        # Check if it has permit fields
        if row.get("permit_number_full"):
            return "建設業許可証"

    # Non-permit: extract from error_reason
    reason = row.get("error_reason", "")
    m = re.search(r"document_type=(\S+?)（許可証以外）", reason)
    if m:
        raw_type = m.group(1)
        return DOC_TYPE_MAP.get(raw_type)

    return None


def _extract_company_from_filename(filename: str) -> str:
    """Fallback: extract company name from filename pattern like 御取引条件等説明書_会社名.pdf"""
    # Pattern: 御取引条件等説明書_COMPANY.pdf or 御取引条件等説明書 COMPANY.pdf
    m = re.search(r"[御お]取引条件等説明書[_＿\s]+(.+?)\.pdf", filename, re.IGNORECASE)
    if m:
        company = m.group(1)
        # Remove extra extensions like .pdf.pdf
        company = re.sub(r"\.pdf$", "", company, flags=re.IGNORECASE)
        # Remove date suffixes like 2026.2
        company = re.sub(r"\s*\d{4}\.\d+$", "", company)
        return _normalize_company_name(company)
    return ""


def process_staging_csvs(csv_paths: list[Path]) -> dict[str, dict[str, list[str]]]:
    """
    Process staging CSVs and return company → {doc_type: [source_files]} mapping.
    """
    company_docs: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    for csv_path in csv_paths:
        with csv_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                source = row.get("source_file", "")

                # Get company name
                company = row.get("company_name_normalized", "").strip()
                if not company:
                    company = _normalize_company_name(row.get("company_name_raw", ""))
                if not company:
                    company = _extract_company_from_filename(source)
                if not company:
                    company = "(不明)"

                # Get document type
                doc_type = _extract_doc_type(row)
                if doc_type:
                    company_docs[company][doc_type].append(source)

    return dict(company_docs)


def generate_checklist_csv(
    company_docs: dict[str, dict[str, list[str]]],
    output_path: Path,
) -> Path:
    """Generate checklist CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = ["会社名"] + REQUIRED_DOCUMENTS + ["提出率", "不足書類"]

    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for company in sorted(company_docs.keys()):
            docs = company_docs[company]
            row = [company]
            submitted = 0
            missing = []

            for doc_type in REQUIRED_DOCUMENTS:
                if doc_type in docs:
                    row.append(f"○ ({len(docs[doc_type])}件)")
                    submitted += 1
                else:
                    row.append("×")
                    missing.append(doc_type)

            rate = f"{submitted}/{len(REQUIRED_DOCUMENTS)} ({submitted/len(REQUIRED_DOCUMENTS)*100:.0f}%)"
            row.append(rate)
            row.append("、".join(missing) if missing else "完備")
            writer.writerow(row)

    return output_path


def print_checklist_summary(company_docs: dict[str, dict[str, list[str]]]) -> None:
    """Print human-readable checklist to stdout."""
    import io, sys as _sys
    out = io.TextIOWrapper(_sys.stdout.buffer, encoding="utf-8", errors="replace")
    out.write(f"\n{'='*80}\n")
    out.write(f"必要書類チェックリスト - {len(company_docs)} 社\n")
    out.write(f"{'='*80}\n\n")

    # Sort by submission rate (ascending = worst first)
    def sort_key(item: tuple[str, dict[str, list[str]]]) -> int:
        return sum(1 for d in REQUIRED_DOCUMENTS if d in item[1])

    for company, docs in sorted(company_docs.items(), key=sort_key):
        submitted = sum(1 for d in REQUIRED_DOCUMENTS if d in docs)
        total = len(REQUIRED_DOCUMENTS)
        bar = "#" * submitted + "." * (total - submitted)
        out.write(f"  {company}\n")
        out.write(f"    [{bar}] {submitted}/{total}\n")

        for doc_type in REQUIRED_DOCUMENTS:
            if doc_type in docs:
                files = docs[doc_type]
                out.write(f"      o {doc_type} ({len(files)})\n")
            else:
                out.write(f"      x {doc_type}\n")
        out.write("\n")

    # Summary stats
    complete = sum(1 for docs in company_docs.values()
                   if all(d in docs for d in REQUIRED_DOCUMENTS))
    out.write(f"{'='*80}\n")
    out.write(f"  全書類完備: {complete}/{len(company_docs)} 社\n")
    out.write(f"{'='*80}\n\n")
    out.flush()


def find_staging_csvs(staging_dir: Path, use_all: bool = False) -> list[Path]:
    """Find staging CSV files."""
    csvs = sorted(staging_dir.glob("staging_permits_*.csv"), reverse=True)
    if not csvs:
        raise FileNotFoundError(f"staging CSV が見つかりません: {staging_dir}")
    if use_all:
        return csvs
    return [csvs[0]]  # latest only


def main() -> None:
    parser = argparse.ArgumentParser(description="必要書類チェックリスト生成")
    parser.add_argument("csv_path", nargs="?", help="staging CSV パス（省略時は最新）")
    parser.add_argument("--all", action="store_true", help="output/ 内の全staging CSVを統合")
    args = parser.parse_args()

    config = load_config()
    data_root = Path(config["DATA_ROOT"])
    staging_dir = data_root / config.get("STAGING_CSV_DIR", "output")

    if args.csv_path:
        csv_paths = [Path(args.csv_path)]
    else:
        csv_paths = find_staging_csvs(staging_dir, use_all=args.all)

    print(f"処理対象CSV: {len(csv_paths)} ファイル")
    for p in csv_paths:
        print(f"  {p.name}")

    company_docs = process_staging_csvs(csv_paths)

    # Generate CSV output
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = staging_dir / f"document_checklist_{ts}.csv"
    generate_checklist_csv(company_docs, output_path)
    print(f"\nチェックリストCSV出力: file:///{str(output_path).replace(chr(92), '/')}")

    # Print summary to stdout
    print_checklist_summary(company_docs)


if __name__ == "__main__":
    main()
