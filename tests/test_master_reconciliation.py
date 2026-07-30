from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from master_reconciliation import (  # noqa: E402
    build_system_companies,
    normalize_company_name,
    normalize_vendor_no,
    reconcile,
)


def test_normalization_preserves_corporate_type() -> None:
    assert normalize_company_name("（株） テスト") == "株式会社テスト"
    assert normalize_company_name("（有） テスト") == "有限会社テスト"
    assert normalize_company_name("（株） テスト") != normalize_company_name("（有） テスト")


@pytest.mark.parametrize("value", ["", "-", "－", "―", None])
def test_blank_vendor_number(value: object) -> None:
    assert normalize_vendor_no(value) == ""


def test_reconcile_exact_and_normalized_candidate() -> None:
    companies = [
        {"company_id": "C0001", "company_name_raw": "株式会社テスト"},
        {"company_id": "C0002", "company_name_raw": "有限会社サンプル"},
    ]
    system = build_system_companies(companies, [], [])
    excel = [
        {
            "source_row": 2,
            "vendor_no": "101",
            "company_name_raw": "株式会社テスト",
            "company_name_normalized": normalize_company_name("株式会社テスト"),
        },
        {
            "source_row": 3,
            "vendor_no": "",
            "company_name_raw": "（有） サンプル",
            "company_name_normalized": normalize_company_name("（有） サンプル"),
        },
    ]
    staging, system_only, summary = reconcile(
        excel, system, "hash", "2026-07-30T00:00:00+09:00"
    )
    assert staging[0]["classification"] == "MATCHED"
    assert staging[0]["matched_company_id"] == "C0001"
    assert staging[1]["classification"] == "REVIEW_REQUIRED"
    assert staging[1]["matched_company_id"] == "C0002"
    assert system_only == []
    assert summary["excel_vendor_no_blank_count"] == 1
    assert not summary["approval_ready"]


def test_duplicate_vendor_number_is_not_auto_matched() -> None:
    excel = [
        {
            "source_row": 2,
            "vendor_no": "101",
            "company_name_raw": "A",
            "company_name_normalized": "a",
        },
        {
            "source_row": 3,
            "vendor_no": "101",
            "company_name_raw": "B",
            "company_name_normalized": "b",
        },
    ]
    staging, _, summary = reconcile(excel, {}, "hash", "now")
    assert [row["classification"] for row in staging] == ["DUPLICATE", "DUPLICATE"]
    assert summary["classification_counts"]["DUPLICATE"] == 2
    assert not summary["approval_ready"]


def test_permit_only_company_is_preserved_as_system_only() -> None:
    system = build_system_companies(
        [],
        [{"company_id": "C0017", "company_name_raw": "許可会社"}],
        [],
    )
    _, system_only, summary = reconcile([], system, "hash", "now")
    assert system_only[0]["company_id"] == "C0017"
    assert system_only[0]["classification"] == "SYSTEM_ONLY"
    assert summary["system_only_count"] == 1
