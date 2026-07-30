from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "reconcile_permit_master.py"
SPEC = importlib.util.spec_from_file_location("reconcile_permit_master", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def companies() -> list[dict[str, str]]:
    return [
        {
            "company_id": "C0001",
            "company_name_raw": "株式会社A",
            "status": "ACTIVE",
            "vendor_no": "1",
        },
        {
            "company_id": "C0002",
            "company_name_raw": "株式会社B",
            "status": "ACTIVE",
            "vendor_no": "2",
        },
        {
            "company_id": "C0003",
            "company_name_raw": "株式会社C",
            "status": "INACTIVE",
            "vendor_no": "3",
        },
    ]


def test_reconciles_exact_and_mlit_only_without_auto_approval() -> None:
    permits = [
        {
            "permit_id": "P1",
            "company_id": "C0001",
            "permit_authority_name_normalized": "愛知県知事",
            "contractor_number": "00123",
            "permit_category": "一般",
            "expiry_date": "2030-01-01",
        }
    ]
    mlit = [
        {
            "company_id": "C0001",
            "authority": "愛知県知事",
            "permit_number": 123.0,
            "category": "般",
            "expiry_date": "2030-01-01T00:00:00",
        },
        {
            "company_id": "C0002",
            "authority": "愛知県知事",
            "permit_number": 456.0,
            "category": "特",
            "expiry_date": "2031-02-03T00:00:00",
            "fetch_status": "OK",
        },
        {
            "company_id": "C0003",
            "authority": "愛知県知事",
            "permit_number": 789.0,
            "category": "般",
            "expiry_date": "2031-02-03T00:00:00",
        },
    ]

    reconciliation, staging, monitoring, summary = MODULE.reconcile(
        permits, mlit, companies()
    )
    classes = {row["company_id"]: row["classification"] for row in reconciliation}
    assert classes == {
        "C0001": "MATCHED",
        "C0002": "MLIT_ONLY",
        "C0003": "INACTIVE_COMPANY",
    }
    assert len(staging) == 1
    assert staging[0]["company_id"] == "C0002"
    assert staging[0]["review_status"] == "PENDING"
    assert staging[0]["permit_data_version"] == 1
    assert summary["company_union"] == 3
    assert sum(row["review_status"] == "PENDING" for row in monitoring) == 2


def test_expiry_mismatch_is_conflict_and_does_not_replace_permit() -> None:
    permits = [
        {
            "permit_id": "P1",
            "company_id": "C0001",
            "permit_authority_name": "愛知県知事",
            "contractor_number": "123",
            "permit_category": "一般",
            "expiry_date": "2030-01-01",
        }
    ]
    mlit = [
        {
            "company_id": "C0001",
            "authority": "愛知県知事",
            "permit_number": "123",
            "category": "一般",
            "expiry_date": "2035-01-01",
        }
    ]
    reconciliation, staging, _, _ = MODULE.reconcile(permits, mlit, companies())
    assert reconciliation[0]["classification"] == "CONFLICT"
    assert reconciliation[0]["proposed_action"] == "KEEP_PERMITS_AND_REVIEW_DIFF"
    assert staging == []


def test_duplicate_upsert_key_requires_review() -> None:
    duplicate = {
        "company_id": "C0002",
        "authority": "愛知県知事",
        "permit_number": "456",
        "category": "一般",
        "expiry_date": "2030-01-01",
    }
    reconciliation, staging, _, _ = MODULE.reconcile(
        [], [duplicate, dict(duplicate)], companies()
    )
    assert reconciliation[0]["classification"] == "DUPLICATE"
    assert len(staging) == 2
    assert all(row["review_status"] == "PENDING" for row in staging)
