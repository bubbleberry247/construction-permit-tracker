"""
generate_company_master.py — staging CSV から会社マスタ CSV を生成する。

ocr_permit.py の match_company() が参照する companies_import_latest.csv を出力する。
staging CSV 内の全レコード（SKIP含む）から company_name_normalized を抽出し、
ユニークな会社名に company_id を採番する。

Usage:
    python src/generate_company_master.py                    # 最新 staging CSV
    python src/generate_company_master.py --all              # 全 staging CSV を統合
    python src/generate_company_master.py path/to/csv        # 指定 CSV
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).parent.parent


def load_config() -> dict[str, Any]:
    config_path = _PROJECT_ROOT / "config.json"
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


def collect_companies(csv_paths: list[Path]) -> dict[str, dict[str, Any]]:
    """
    staging CSV から会社名を収集。
    Returns: {normalized_name: {"raw_names": set, "doc_types": set, "source_files": list}}
    """
    companies: dict[str, dict[str, Any]] = {}

    for csv_path in csv_paths:
        with csv_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                name_normalized = (row.get("company_name_normalized") or "").strip()
                name_raw = (row.get("company_name_raw") or "").strip()
                if not name_normalized and not name_raw:
                    continue
                if not name_normalized:
                    name_normalized = name_raw

                if name_normalized not in companies:
                    companies[name_normalized] = {
                        "raw_names": set(),
                        "doc_types": set(),
                        "source_files": [],
                        "has_permit": False,
                    }

                if name_raw:
                    companies[name_normalized]["raw_names"].add(name_raw)
                companies[name_normalized]["source_files"].append(
                    row.get("source_file", "")
                )

                # Track document types
                status = row.get("parse_status", "")
                if status in ("OK", "REVIEW_NEEDED") and row.get("permit_number_full"):
                    companies[name_normalized]["has_permit"] = True
                    companies[name_normalized]["doc_types"].add("建設業許可証")

                reason = row.get("error_reason", "")
                if "document_type=" in reason:
                    import re

                    m = re.search(r"document_type=(\S+?)（", reason)
                    if m:
                        companies[name_normalized]["doc_types"].add(m.group(1))

    return companies


def write_company_master(
    companies: dict[str, dict[str, Any]], output_dir: Path
) -> Path:
    """companies_import_latest.csv を出力。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "companies_import_latest.csv"

    # Also write timestamped version
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts_path = output_dir / f"companies_import_{ts}.csv"

    headers = [
        "company_id",
        "company_name_normalized",
        "company_name_raw",
        "has_permit",
        "document_count",
        "created_at",
    ]

    rows: list[dict[str, str]] = []
    for i, (name, info) in enumerate(sorted(companies.items()), start=1):
        company_id = f"C{i:04d}"
        raw_names = (
            " / ".join(sorted(info["raw_names"])) if info["raw_names"] else name
        )
        rows.append(
            {
                "company_id": company_id,
                "company_name_normalized": name,
                "company_name_raw": raw_names,
                "has_permit": "TRUE" if info["has_permit"] else "FALSE",
                "document_count": str(len(info["source_files"])),
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
        )

    for path in (output_path, ts_path):
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="会社マスタCSV生成")
    parser.add_argument("csv_path", nargs="?", help="staging CSV パス")
    parser.add_argument("--all", action="store_true", help="全staging CSVを統合")
    args = parser.parse_args()

    config = load_config()
    data_root = Path(config["DATA_ROOT"])
    staging_dir = data_root / config.get("STAGING_CSV_DIR", "output")

    if args.csv_path:
        csv_paths = [Path(args.csv_path)]
    elif args.all:
        csv_paths = sorted(staging_dir.glob("staging_permits_*.csv"))
    else:
        csvs = sorted(staging_dir.glob("staging_permits_*.csv"), reverse=True)
        csv_paths = [csvs[0]] if csvs else []

    if not csv_paths:
        print("staging CSV が見つかりません")
        sys.exit(1)

    print(f"処理対象: {len(csv_paths)} ファイル")
    companies = collect_companies(csv_paths)
    print(f"ユニーク会社数: {len(companies)}")

    output_path = write_company_master(companies, staging_dir)
    print(f"会社マスタ出力: file:///{str(output_path).replace(chr(92), '/')}")

    # Summary
    with_permit = sum(1 for c in companies.values() if c["has_permit"])
    print(f"\n  許可証あり: {with_permit} 社")
    print(f"  許可証なし: {len(companies) - with_permit} 社")
    print(f"  合計: {len(companies)} 社")


if __name__ == "__main__":
    main()
