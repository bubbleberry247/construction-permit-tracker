#!/usr/bin/env python3
"""Reconcile canonical Permits with legacy MLIT observations.

This command is read-only against its inputs. It emits review staging files and
never promotes an MLIT observation into the canonical permit master by itself.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable


def load_records(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    records = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise ValueError(f"records array not found: {path}")
    return [dict(record) for record in records]


def load_companies(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def normalize_authority(value: Any) -> str:
    return re.sub(r"[\s\u3000]+", "", str(value or "")).strip()


def normalize_category(value: Any) -> str:
    text = re.sub(r"[\s\u3000]+", "", str(value or ""))
    return {
        "般": "一般",
        "一般": "一般",
        "特": "特定",
        "特定": "特定",
    }.get(text, text)


def normalize_number(value: Any, fallback: Any = "") -> str:
    raw = value if value not in (None, "") else fallback
    if isinstance(raw, float) and raw.is_integer():
        return str(int(raw))
    text = str(raw or "").strip()
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    groups = re.findall(r"\d+", text)
    if not groups:
        return ""
    normalized = groups[-1].lstrip("0")
    return normalized or "0"


def normalize_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    text = str(value).strip()
    matched = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
    if not matched:
        return ""
    try:
        parsed = date(*map(int, matched.groups()))
    except ValueError:
        return ""
    return parsed.isoformat()


def stable_key(record: dict[str, Any], source: str) -> str:
    company_id = str(record.get("company_id") or "").strip()
    if source == "PERMIT":
        authority = normalize_authority(
            record.get("permit_authority_name_normalized")
            or record.get("permit_authority_name")
        )
        number = normalize_number(
            record.get("contractor_number"), record.get("permit_number_full")
        )
        category = normalize_category(record.get("permit_category"))
    else:
        authority = normalize_authority(record.get("authority"))
        number = normalize_number(record.get("permit_number"))
        category = normalize_category(record.get("category"))
    return "|".join((company_id, authority, number, category))


def record_hash(record: dict[str, Any]) -> str:
    serialized = json.dumps(
        record, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def reconcile(
    permits: list[dict[str, Any]],
    mlit_rows: list[dict[str, Any]],
    companies: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    company_map = {row["company_id"].strip(): row for row in companies}
    permits_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    mlit_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in permits:
        permits_by_key[stable_key(row, "PERMIT")].append(row)
    for row in mlit_rows:
        mlit_by_key[stable_key(row, "MLIT")].append(row)

    reconciliation: list[dict[str, Any]] = []
    import_staging: list[dict[str, Any]] = []
    class_counts: Counter[str] = Counter()
    all_keys = sorted(set(permits_by_key) | set(mlit_by_key))

    for index, key in enumerate(all_keys, start=1):
        existing = permits_by_key.get(key, [])
        observed = mlit_by_key.get(key, [])
        company_id, authority, number, category = key.split("|", 3)
        company = company_map.get(company_id, {})
        inactive = str(company.get("status") or "").upper() != "ACTIVE"
        approved_expiry = normalize_date(existing[0].get("expiry_date")) if existing else ""
        observed_expiry = normalize_date(observed[0].get("expiry_date")) if observed else ""
        risk_flags: list[str] = []

        if not company_id or not authority or not number:
            classification = "REVIEW_REQUIRED"
            proposed_action = "FIX_IDENTIFIER"
            risk_flags.append("IDENTIFIER_MISSING")
        elif len(existing) > 1 or len(observed) > 1:
            classification = "DUPLICATE"
            proposed_action = "MANUAL_REVIEW"
            risk_flags.append("DUPLICATE_UPSERT_KEY")
        elif inactive:
            classification = "INACTIVE_COMPANY"
            proposed_action = "DO_NOT_MIGRATE"
        elif existing and observed:
            if approved_expiry and observed_expiry and approved_expiry != observed_expiry:
                classification = "CONFLICT"
                proposed_action = "KEEP_PERMITS_AND_REVIEW_DIFF"
                risk_flags.append("EXPIRY_MISMATCH")
            else:
                classification = "MATCHED"
                proposed_action = "KEEP_EXISTING"
        elif existing:
            classification = "PERMIT_ONLY"
            proposed_action = "KEEP_EXISTING_AND_SEED_MLIT"
        else:
            classification = "MLIT_ONLY"
            proposed_action = "CREATE_PERMIT_CANDIDATE"

        class_counts[classification] += 1
        reconciliation.append(
            {
                "reconciliation_id": f"PR-{index:04d}",
                "stable_key": key,
                "company_id": company_id,
                "company_name": company.get("company_name_raw", ""),
                "company_status": company.get("status", ""),
                "authority": authority,
                "contractor_number": number,
                "permit_category": category,
                "permit_count": len(existing),
                "mlit_count": len(observed),
                "permit_ids": ",".join(
                    str(row.get("permit_id") or "") for row in existing
                ),
                "approved_expiry_date": approved_expiry,
                "observed_expiry_date": observed_expiry,
                "classification": classification,
                "proposed_action": proposed_action,
                "risk_flags": ",".join(risk_flags),
                "review_status": (
                    "READY_FOR_APPROVAL"
                    if classification in {"MATCHED", "PERMIT_ONLY", "INACTIVE_COMPANY"}
                    else "PENDING"
                ),
                "reviewer": "",
                "reviewed_at": "",
                "review_note": "",
            }
        )

        if classification not in {"MLIT_ONLY", "REVIEW_REQUIRED", "DUPLICATE"}:
            continue
        for mlit_index, row in enumerate(observed, start=1):
            source_hash = record_hash(row)
            expiry = normalize_date(row.get("expiry_date"))
            current_status = "EXPIRED" if expiry and expiry < date.today().isoformat() else "VALID"
            trade_parts = [
                str(row.get("trades_ippan") or "").strip(),
                str(row.get("trades_tokutei") or "").strip(),
            ]
            import_staging.append(
                {
                    "migration_row_id": f"PM-{index:04d}-{mlit_index:02d}",
                    "source_sha256": source_hash,
                    "stable_key": key,
                    "classification": classification,
                    "review_status": "PENDING",
                    "reviewer": "",
                    "reviewed_at": "",
                    "review_note": "",
                    "permit_id": "",
                    "company_id": company_id,
                    "company_name_raw": company.get("company_name_raw", ""),
                    "permit_authority_name": authority,
                    "permit_authority_name_normalized": authority,
                    "permit_authority_type": (
                        "大臣" if "大臣" in authority else "知事" if "知事" in authority else ""
                    ),
                    "permit_category": category,
                    "permit_year": "",
                    "contractor_number": number,
                    "permit_number_full": f"{authority} 許可 第{number}号",
                    "trade_categories": "|".join(part for part in trade_parts if part),
                    "issue_date": "",
                    "expiry_date": expiry,
                    "renewal_deadline_date": "",
                    "current_status": current_status,
                    "mlit_confirmed_date": normalize_date(row.get("last_synced")),
                    "mlit_confirm_result": str(row.get("fetch_status") or ""),
                    "mlit_screenshot_url": "",
                    "source_file": "MLIT_LEGACY_SNAPSHOT",
                    "source_file_hash": source_hash,
                    "parse_status": "MIGRATION_REVIEW_REQUIRED",
                    "note": "旧MLIT観測からの許可正本候補。人手承認前は適用禁止。",
                    "permit_data_version": 1,
                }
            )

    permit_company_ids = {
        str(row.get("company_id") or "").strip() for row in permits
    } - {""}
    mlit_company_ids = {
        str(row.get("company_id") or "").strip() for row in mlit_rows
    } - {""}
    monitoring_staging: list[dict[str, Any]] = []
    for company in sorted(companies, key=lambda row: row.get("company_id", "")):
        company_id = str(company.get("company_id") or "").strip()
        active = str(company.get("status") or "").upper() == "ACTIVE"
        referenced = company_id in permit_company_ids or company_id in mlit_company_ids
        proposed = active and referenced
        monitoring_staging.append(
            {
                "company_id": company_id,
                "vendor_no": company.get("vendor_no", ""),
                "company_name_raw": company.get("company_name_raw", ""),
                "status": company.get("status", ""),
                "has_permit": str(company_id in permit_company_ids).upper(),
                "has_mlit_observation": str(company_id in mlit_company_ids).upper(),
                "proposed_monitoring_enabled": str(proposed).upper(),
                "proposed_action": "MONITOR" if proposed else "DO_NOT_MONITOR",
                "review_status": "PENDING" if proposed else "AUTO_APPROVED",
                "reviewer": "",
                "reviewed_at": "",
                "review_note": (
                    "" if proposed else "許可参照なし、または会社statusがACTIVEではない"
                ),
            }
        )

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "permit_rows": len(permits),
        "permit_companies": len(permit_company_ids),
        "mlit_rows": len(mlit_rows),
        "mlit_companies": len(mlit_company_ids),
        "company_union": len(permit_company_ids | mlit_company_ids),
        "company_intersection": len(permit_company_ids & mlit_company_ids),
        "mlit_only_companies": len(mlit_company_ids - permit_company_ids),
        "permit_only_companies": len(permit_company_ids - mlit_company_ids),
        "reconciliation_keys": len(all_keys),
        "classification_counts": dict(sorted(class_counts.items())),
        "permit_import_candidates": len(import_staging),
        "monitoring_review_required": sum(
            row["review_status"] == "PENDING" for row in monitoring_staging
        ),
        "monitoring_auto_excluded": sum(
            row["review_status"] == "AUTO_APPROVED" for row in monitoring_staging
        ),
    }
    return reconciliation, import_staging, monitoring_staging, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--permits-json", type=Path, required=True)
    parser.add_argument("--mlit-json", type=Path, required=True)
    parser.add_argument("--companies-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise ValueError("output directory must be absent or empty")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    reconciliation, import_staging, monitoring, summary = reconcile(
        load_records(args.permits_json),
        load_records(args.mlit_json),
        load_companies(args.companies_csv),
    )
    write_csv(
        args.output_dir / "permit_reconciliation.csv",
        reconciliation,
        list(reconciliation[0]) if reconciliation else [],
    )
    write_csv(
        args.output_dir / "permit_import_staging.csv",
        import_staging,
        list(import_staging[0]) if import_staging else [],
    )
    write_csv(
        args.output_dir / "monitoring_target_staging.csv",
        monitoring,
        list(monitoring[0]) if monitoring else [],
    )
    (args.output_dir / "permit_reconciliation_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
