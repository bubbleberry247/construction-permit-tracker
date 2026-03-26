"""
migrate_company_master.py — 旧 companies_import_latest.csv + staging CSVs から
新スキーマの company_master.csv と company_id_redirect.csv を生成する。

処理フロー:
  1. 旧マスタ CSV 読み込み
  2. staging CSV から許可証データ収集 (parse_status=OK)
  3. 除外ルール適用 (テスト/非建設データ)
  4. マージルール適用 (重複ペア統合)
  5. company_master.csv 出力 (新スキーマ, UTF-8 BOM)
  6. company_id_redirect.csv 出力 (UTF-8 BOM)

Usage:
    python src/migrate_company_master.py
"""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).parent.parent
_OUTPUT_DIR = _PROJECT_ROOT / "output"

# --- Exclusion rules ---
EXCLUDED_IDS: dict[str, str] = {
    "C0001": "test data",
    "C0002": "test data",
    "C0004": "test data",
    "C0005": "test data",
}

# --- Merge rules: (keep_id, discard_id, reason) ---
# keep_id = lower ID; inherit richer data from whichever has permit info
MERGE_RULES: list[tuple[str, str, str]] = [
    ("C0009", "C0010", "duplicate (honorific)"),
    ("C0014", "C0015", "duplicate (space)"),
    ("C0021", "C0026", "duplicate (space+kana)"),
    ("C0022", "C0027", "duplicate (space)"),
    ("C0034", "C0044", "duplicate (space)"),
]

# --- Official name overrides for merged pairs ---
# Use the cleaner normalized name (without extra spaces)
OFFICIAL_NAME_OVERRIDES: dict[str, str] = {
    "C0009": "中根 一仁",
    "C0014": "名西建材株式会社",
    "C0021": "有限会社YAMAJIパーティション",
    "C0022": "有限会社キー・カンパニー",
    "C0034": "株式会社メンテック",
}

# New schema headers
MASTER_HEADERS: list[str] = [
    "company_id",
    "official_name",
    "name_aliases",
    "corporation_type",
    "sender_emails",
    "contact_person",
    "permit_number",
    "permit_authority",
    "mlit_status",
    "last_confirmed_at",
    "status",
    "created_at",
    "updated_at",
]

REDIRECT_HEADERS: list[str] = [
    "old_company_id",
    "new_company_id",
    "reason",
]


def load_old_master(path: Path) -> list[dict[str, str]]:
    """旧 companies_import_latest.csv を読み込む。"""
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def load_permit_data(output_dir: Path) -> dict[str, dict[str, str]]:
    """
    全 staging CSV から parse_status=OK の許可証データを収集。
    Returns: {company_name_normalized: {permit_authority, contractor_number, ...}}
    最新のデータ（最後に出現したもの）を採用。
    """
    permits: dict[str, dict[str, str]] = {}
    staging_files = sorted(output_dir.glob("staging_permits_*.csv"))

    for csv_path in staging_files:
        with csv_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("parse_status") != "OK":
                    continue
                name = (row.get("company_name_normalized") or "").strip()
                if not name:
                    continue
                permits[name] = {
                    "permit_authority": (row.get("permit_authority_name_normalized") or "").strip(),
                    "contractor_number": (row.get("contractor_number") or "").strip(),
                    "permit_number_full": (row.get("permit_number_full") or "").strip(),
                    "company_id": (row.get("company_id") or "").strip(),
                }
    return permits


def classify_corporation(name: str) -> str:
    """法人区分を判定。"""
    corp_markers = ["株式会社", "有限会社", "合同会社", "合資会社", "合名会社",
                    "一般社団法人", "一般財団法人", "協同組合"]
    for marker in corp_markers:
        if marker in name:
            return "CORPORATION"
    return "SOLE_PROPRIETOR"


def collect_raw_names(row: dict[str, str]) -> set[str]:
    """旧マスタの company_name_raw からエイリアス候補を収集。"""
    raw = (row.get("company_name_raw") or "").strip()
    if not raw:
        return set()
    # " / " 区切りで複数の表記揺れが入っている
    return {n.strip() for n in raw.split(" / ") if n.strip()}


def safe_write_csv(
    path: Path,
    headers: list[str],
    rows: list[dict[str, str]],
) -> None:
    """temp file + rename で安全に CSV を書き込む (UTF-8 BOM)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".csv",
        prefix=path.stem + "_tmp_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        # Windows: 上書き対象が存在する場合は先に削除
        if path.exists():
            path.unlink()
        Path(tmp_path).rename(path)
    except Exception:
        # 失敗時は temp ファイルをクリーンアップ
        try:
            Path(tmp_path).unlink()
        except OSError:
            pass
        raise


def migrate() -> None:
    old_master_path = _OUTPUT_DIR / "companies_import_latest.csv"
    if not old_master_path.exists():
        print(f"ERROR: 旧マスタが見つかりません: {old_master_path}")
        sys.exit(1)

    # --- Step 1: Load old master ---
    old_rows = load_old_master(old_master_path)
    print(f"旧マスタ読み込み: {len(old_rows)} 社")

    # Index by company_id
    old_by_id: dict[str, dict[str, str]] = {r["company_id"]: r for r in old_rows}

    # --- Step 2: Load permit data from staging CSVs ---
    permits = load_permit_data(_OUTPUT_DIR)
    print(f"許可証データ (parse_status=OK): {len(permits)} 件")

    # --- Step 3: Build merge mapping ---
    # discard_id -> keep_id
    merge_map: dict[str, str] = {}
    merge_reasons: dict[str, str] = {}
    for keep_id, discard_id, reason in MERGE_RULES:
        merge_map[discard_id] = keep_id
        merge_reasons[discard_id] = reason

    # --- Step 4: Build redirect records ---
    redirect_rows: list[dict[str, str]] = []

    # Exclusions
    for cid, reason in sorted(EXCLUDED_IDS.items()):
        redirect_rows.append({
            "old_company_id": cid,
            "new_company_id": "EXCLUDED",
            "reason": reason,
        })

    # Merges
    for discard_id, keep_id in sorted(merge_map.items()):
        redirect_rows.append({
            "old_company_id": discard_id,
            "new_company_id": keep_id,
            "reason": merge_reasons[discard_id],
        })

    # --- Step 5: Build new master rows ---
    now_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    master_rows: list[dict[str, str]] = []

    # Track which IDs to skip
    skip_ids = set(EXCLUDED_IDS.keys()) | set(merge_map.keys())

    for old_row in old_rows:
        cid = old_row["company_id"]
        if cid in skip_ids:
            continue

        # Collect aliases from this record
        aliases: set[str] = collect_raw_names(old_row)
        # Also add the normalized name itself as an alias candidate
        norm_name = (old_row.get("company_name_normalized") or "").strip()
        if norm_name:
            aliases.add(norm_name)

        # Determine has_permit and permit info
        has_permit = old_row.get("has_permit", "FALSE") == "TRUE"
        permit_number = ""
        permit_authority = ""

        # Check if this is a merge target — absorb the discarded record's data
        for discard_id, keep_id in merge_map.items():
            if keep_id == cid:
                discard_row = old_by_id.get(discard_id, {})
                # Inherit raw names from discarded record
                aliases |= collect_raw_names(discard_row)
                discard_norm = (discard_row.get("company_name_normalized") or "").strip()
                if discard_norm:
                    aliases.add(discard_norm)
                # Inherit has_permit if discarded had it
                if discard_row.get("has_permit") == "TRUE":
                    has_permit = True

        # Look up permit data from staging CSVs
        # Try matching by normalized name (check both current and absorbed names)
        all_candidate_names = {norm_name} | aliases
        for candidate_name in all_candidate_names:
            if candidate_name in permits:
                permit_number = permits[candidate_name]["contractor_number"]
                permit_authority = permits[candidate_name]["permit_authority"]
                break

        # If this company had permit data under a discarded ID's name, find it
        if not permit_number:
            for discard_id, keep_id in merge_map.items():
                if keep_id == cid:
                    discard_row = old_by_id.get(discard_id, {})
                    discard_norm = (discard_row.get("company_name_normalized") or "").strip()
                    if discard_norm in permits:
                        permit_number = permits[discard_norm]["contractor_number"]
                        permit_authority = permits[discard_norm]["permit_authority"]
                        break

        # Determine official name (override for merged pairs, otherwise normalized)
        official_name = OFFICIAL_NAME_OVERRIDES.get(cid, norm_name)

        # Remove official_name from aliases to avoid redundancy, then sort
        aliases.discard(official_name)
        alias_list = sorted(aliases)
        aliases_str = "|".join(alias_list)

        # Corporation type
        corp_type = classify_corporation(official_name)

        master_rows.append({
            "company_id": cid,
            "official_name": official_name,
            "name_aliases": aliases_str,
            "corporation_type": corp_type,
            "sender_emails": "",
            "contact_person": "",
            "permit_number": permit_number,
            "permit_authority": permit_authority,
            "mlit_status": "NOT_CONFIRMED",
            "last_confirmed_at": "",
            "status": "ACTIVE",
            "created_at": now_ts,
            "updated_at": now_ts,
        })

    # --- Step 6: Write outputs ---
    master_path = _OUTPUT_DIR / "company_master.csv"
    safe_write_csv(master_path, MASTER_HEADERS, master_rows)
    print(f"company_master.csv 出力: {len(master_rows)} 社")
    print(f"  file:///{str(master_path).replace(chr(92), '/')}")

    redirect_path = _OUTPUT_DIR / "company_id_redirect.csv"
    safe_write_csv(redirect_path, REDIRECT_HEADERS, redirect_rows)
    print(f"company_id_redirect.csv 出力: {len(redirect_rows)} 件")
    print(f"  file:///{str(redirect_path).replace(chr(92), '/')}")

    # --- Step 7: Summary ---
    excluded_count = len(EXCLUDED_IDS)
    merged_count = len(MERGE_RULES)
    with_permits = sum(1 for r in master_rows if r["permit_number"])
    corp_count = sum(1 for r in master_rows if r["corporation_type"] == "CORPORATION")
    sole_count = sum(1 for r in master_rows if r["corporation_type"] == "SOLE_PROPRIETOR")

    print("\n=== Migration Summary ===")
    print(f"  旧マスタ会社数:    {len(old_rows)}")
    print(f"  除外:              {excluded_count} 社")
    print(f"  マージ:            {merged_count} ペア")
    print(f"  新マスタ会社数:    {len(master_rows)} 社")
    print(f"  許可証あり:        {with_permits} 社")
    print(f"  法人:              {corp_count} 社")
    print(f"  個人事業主:        {sole_count} 社")


if __name__ == "__main__":
    migrate()
