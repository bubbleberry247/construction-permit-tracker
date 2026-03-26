"""
generate_company_master.py — 会社マスタCSV生成（新スキーマ対応）。

staging CSV と既存 company_master.csv から会社マスタを更新する。
新規会社は自動採番、既存会社は許可証データを更新。
また inbound_log.csv から email_company_map.csv を再生成する。

Usage:
    python src/generate_company_master.py                 # 最新 staging CSV
    python src/generate_company_master.py --all            # 全 staging CSV 統合
    python src/generate_company_master.py path/to/csv      # 指定 CSV
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# New schema headers
# ---------------------------------------------------------------------------
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

# Old format headers (backward compatibility)
OLD_HEADERS: list[str] = [
    "company_id",
    "company_name_normalized",
    "company_name_raw",
    "has_permit",
    "document_count",
    "created_at",
]

EMAIL_MAP_HEADERS: list[str] = [
    "sender_email",
    "company_id",
    "match_source",
    "confidence",
    "created_at",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def load_config() -> dict[str, Any]:
    config_path = _PROJECT_ROOT / "config.json"
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------
def normalize_name(name: str) -> str:
    """会社名を正規化する（マッチング用）。

    NFKC正規化 + 空白除去 + 法人格略称を展開。
    """
    if not name:
        return ""
    normalized = unicodedata.normalize("NFKC", name)
    normalized = re.sub(r"\s+", "", normalized)
    normalized = normalized.replace("(株)", "株式会社")
    normalized = normalized.replace("(有)", "有限会社")
    normalized = normalized.replace("(合)", "合同会社")
    return normalized


def classify_corporation(name: str) -> str:
    """法人区分を判定する。"""
    corp_markers = [
        "株式会社", "有限会社", "合同会社", "合資会社", "合名会社",
        "一般社団法人", "一般財団法人", "協同組合",
    ]
    for marker in corp_markers:
        if marker in name:
            return "CORPORATION"
    return "SOLE_PROPRIETOR"


# ---------------------------------------------------------------------------
# Safe CSV write (temp + rename)
# ---------------------------------------------------------------------------
def safe_write_csv(
    path: Path,
    headers: list[str],
    rows: list[dict[str, str]],
) -> None:
    """一時ファイルに書き込み後、原子的にリネームする (UTF-8 BOM)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".csv",
        prefix=path.stem + "_tmp_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        if path.exists():
            path.unlink()
        Path(tmp_path).rename(path)
    except Exception:
        try:
            Path(tmp_path).unlink()
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Load existing company_master.csv
# ---------------------------------------------------------------------------
def load_existing_master(path: Path) -> list[dict[str, str]]:
    """既存の company_master.csv を読み込む。存在しなければ空リスト。"""
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def get_max_company_id(rows: list[dict[str, str]]) -> int:
    """既存マスタから最大の company_id 番号を取得する。"""
    max_num = 0
    for row in rows:
        cid = row.get("company_id", "")
        if cid.startswith("C") and cid[1:].isdigit():
            max_num = max(max_num, int(cid[1:]))
    return max_num


# ---------------------------------------------------------------------------
# Collect companies from staging CSVs
# ---------------------------------------------------------------------------
def collect_staging_data(
    csv_paths: list[Path],
) -> dict[str, dict[str, Any]]:
    """staging CSV から会社データを収集する。

    Returns:
        {normalized_name: {
            raw_names: set[str],
            source_files: list[str],
            has_permit: bool,
            permit_authority: str,
            contractor_number: str,
            doc_count: int,
        }}
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
                        "source_files": [],
                        "has_permit": False,
                        "permit_authority": "",
                        "contractor_number": "",
                    }

                entry = companies[name_normalized]
                if name_raw:
                    entry["raw_names"].add(name_raw)
                entry["source_files"].append(row.get("source_file", ""))

                # permit data from parse_status=OK records
                if row.get("parse_status") == "OK":
                    entry["has_permit"] = True
                    authority = (row.get("permit_authority_name_normalized") or "").strip()
                    number = (row.get("contractor_number") or "").strip()
                    if authority:
                        entry["permit_authority"] = authority
                    if number:
                        entry["contractor_number"] = number

    return companies


# ---------------------------------------------------------------------------
# Build lookup index from existing master (normalized name -> row)
# ---------------------------------------------------------------------------
def build_master_index(
    master_rows: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    """既存マスタから正規化名 -> 行の辞書を構築する。

    official_name と name_aliases の両方をキーに登録する。
    """
    index: dict[str, dict[str, str]] = {}
    for row in master_rows:
        official = row.get("official_name", "")
        norm_official = normalize_name(official)
        if norm_official:
            index[norm_official] = row

        aliases_str = row.get("name_aliases", "")
        if aliases_str:
            for alias in aliases_str.split("|"):
                alias = alias.strip()
                if alias:
                    norm_alias = normalize_name(alias)
                    if norm_alias and norm_alias not in index:
                        index[norm_alias] = row
    return index


# ---------------------------------------------------------------------------
# Regenerate email_company_map.csv from inbound_log
# ---------------------------------------------------------------------------
def regenerate_email_map(
    log_path: Path,
    master_index: dict[str, dict[str, str]],
    output_path: Path,
) -> int:
    """inbound_log.csv から email_company_map.csv を再生成する。

    Args:
        log_path: inbound_log.csv のパス。
        master_index: normalize_name -> master row の辞書。
        output_path: email_company_map.csv の出力先。

    Returns:
        出力した行数。
    """
    if not log_path.exists():
        # inbound_log が無い場合はヘッダのみ出力
        safe_write_csv(output_path, EMAIL_MAP_HEADERS, [])
        return 0

    # sender_email -> company_id のマップを構築
    email_map: dict[str, dict[str, str]] = {}
    now_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    with log_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            sender = (row.get("original_sender_email") or row.get("sender_email") or "").strip().lower()
            if not sender:
                continue
            # 転送アカウント自身はスキップ
            if sender in ("shinsei.tic@gmail.com", "kalimistk@gmail.com"):
                continue
            if sender in email_map:
                continue

            # ファイル名から会社名を推定してマッチング
            file_name = row.get("file_name", "")
            company_id = ""
            confidence = "LOW"

            if file_name:
                extracted = _extract_company_from_filename(file_name)
                if extracted:
                    norm_extracted = normalize_name(extracted)
                    matched_row = master_index.get(norm_extracted)
                    if matched_row:
                        company_id = matched_row["company_id"]
                        confidence = "HIGH"
                    else:
                        # 部分一致を試行
                        for norm_key, master_row in master_index.items():
                            if norm_extracted in norm_key or norm_key in norm_extracted:
                                company_id = master_row["company_id"]
                                confidence = "MEDIUM"
                                break

            email_map[sender] = {
                "sender_email": sender,
                "company_id": company_id,
                "match_source": "auto_regenerate",
                "confidence": confidence,
                "created_at": now_ts,
            }

    rows = list(email_map.values())
    safe_write_csv(output_path, EMAIL_MAP_HEADERS, rows)
    return len(rows)


def _extract_company_from_filename(file_name: str) -> str:
    """ファイル名から会社名を抽出する。"""
    name = file_name
    name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    name = name.replace("\u300c", "").replace("\u300d", "")  # 「」

    patterns = [
        r"[_\uff3f](.+?)$",
        r"\u8aac\u660e\u66f8[\u3000\s]+(.+?)$",  # 説明書 + 空白
    ]
    for pattern in patterns:
        match = re.search(pattern, name)
        if match:
            company = match.group(1).strip()
            if company:
                return company
    return ""


# ---------------------------------------------------------------------------
# Generate backward-compatible old format
# ---------------------------------------------------------------------------
def write_old_format(
    master_rows: list[dict[str, str]],
    staging_data: dict[str, dict[str, Any]],
    output_dir: Path,
) -> Path:
    """旧形式 companies_import_latest.csv を出力する（後方互換性）。"""
    now_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    old_rows: list[dict[str, str]] = []

    # staging_data から doc_count を引くための正規化インデックス
    staging_norm: dict[str, dict[str, Any]] = {}
    for name, data in staging_data.items():
        norm = normalize_name(name)
        staging_norm[norm] = data

    for row in master_rows:
        official = row.get("official_name", "")
        norm = normalize_name(official)

        # aliases -> raw_names として使う
        aliases_str = row.get("name_aliases", "")
        raw_names = aliases_str.replace("|", " / ") if aliases_str else official

        has_permit = "TRUE" if row.get("permit_number") else "FALSE"

        # staging data からドキュメント数を取得
        doc_count = 0
        staging_entry = staging_norm.get(norm)
        if staging_entry:
            doc_count = len(staging_entry["source_files"])

        old_rows.append({
            "company_id": row["company_id"],
            "company_name_normalized": official,
            "company_name_raw": raw_names,
            "has_permit": has_permit,
            "document_count": str(doc_count),
            "created_at": row.get("created_at", now_ts),
        })

    output_path = output_dir / "companies_import_latest.csv"
    safe_write_csv(output_path, OLD_HEADERS, old_rows)
    return output_path


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------
def generate(csv_paths: list[Path], output_dir: Path) -> None:
    """会社マスタ生成のメインロジック。"""
    now_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # 1. 既存 company_master.csv をベースとして読み込み
    master_path = output_dir / "company_master.csv"
    existing_rows = load_existing_master(master_path)
    existing_index = build_master_index(existing_rows)
    next_id_num = get_max_company_id(existing_rows) + 1

    print(f"既存マスタ: {len(existing_rows)} 社 (次ID: C{next_id_num:04d})")

    # 2. staging CSV から会社データ収集
    staging_data = collect_staging_data(csv_paths)
    print(f"staging CSV 会社数: {len(staging_data)}")

    # 3. 既存マスタの更新 + 新規会社の追加
    # まず既存レコードをコピーし、staging データで更新する
    updated_rows: dict[str, dict[str, str]] = {}
    for row in existing_rows:
        updated_rows[row["company_id"]] = dict(row)

    # staging の各会社を処理
    new_count = 0
    updated_count = 0
    for staging_name, staging_info in staging_data.items():
        norm_staging = normalize_name(staging_name)

        # 既存マスタにマッチするか確認
        matched_row = existing_index.get(norm_staging)

        if matched_row:
            # 既存会社 -> 許可証データを更新
            cid = matched_row["company_id"]
            row = updated_rows[cid]
            changed = False

            # 許可証情報の更新（新データがある場合のみ）
            if staging_info["has_permit"]:
                if staging_info["contractor_number"] and not row.get("permit_number"):
                    row["permit_number"] = staging_info["contractor_number"]
                    changed = True
                if staging_info["permit_authority"] and not row.get("permit_authority"):
                    row["permit_authority"] = staging_info["permit_authority"]
                    changed = True

            # name_aliases の更新（新しい表記揺れを追加）
            existing_aliases_str = row.get("name_aliases", "")
            existing_aliases = set(
                a.strip() for a in existing_aliases_str.split("|") if a.strip()
            ) if existing_aliases_str else set()
            official = row.get("official_name", "")

            new_aliases = set()
            for raw_name in staging_info["raw_names"]:
                if raw_name != official and raw_name not in existing_aliases:
                    new_aliases.add(raw_name)
            if staging_name != official and staging_name not in existing_aliases:
                new_aliases.add(staging_name)

            if new_aliases:
                all_aliases = existing_aliases | new_aliases
                row["name_aliases"] = "|".join(sorted(all_aliases))
                changed = True

            if changed:
                row["updated_at"] = now_ts
                updated_count += 1
        else:
            # 新規会社 -> 自動採番
            cid = f"C{next_id_num:04d}"
            next_id_num += 1

            # official_name: 正規化名を使用
            official_name = staging_name

            # aliases: raw_names から official_name を除いたもの
            aliases = set()
            for raw_name in staging_info["raw_names"]:
                if raw_name != official_name:
                    aliases.add(raw_name)
            aliases_str = "|".join(sorted(aliases))

            corp_type = classify_corporation(official_name)

            updated_rows[cid] = {
                "company_id": cid,
                "official_name": official_name,
                "name_aliases": aliases_str,
                "corporation_type": corp_type,
                "sender_emails": "",
                "contact_person": "",
                "permit_number": staging_info.get("contractor_number", ""),
                "permit_authority": staging_info.get("permit_authority", ""),
                "mlit_status": "NOT_CONFIRMED",
                "last_confirmed_at": "",
                "status": "ACTIVE",
                "created_at": now_ts,
                "updated_at": now_ts,
            }
            # 新規会社を既存インデックスにも追加（後続の重複検出用）
            existing_index[norm_staging] = updated_rows[cid]
            new_count += 1

    # 4. company_id でソートして出力
    master_rows = sorted(updated_rows.values(), key=lambda r: r["company_id"])
    safe_write_csv(master_path, MASTER_HEADERS, master_rows)
    print(f"company_master.csv 出力: {len(master_rows)} 社")
    print(f"  file:///{str(master_path).replace(chr(92), '/')}")

    # 5. 旧形式 companies_import_latest.csv も出力
    old_path = write_old_format(master_rows, staging_data, output_dir)
    print(f"companies_import_latest.csv 出力 (後方互換): {len(master_rows)} 社")
    print(f"  file:///{str(old_path).replace(chr(92), '/')}")

    # 6. email_company_map.csv 再生成
    config = load_config()
    data_root = Path(config["DATA_ROOT"])
    log_path = data_root / "logs" / "inbound_log.csv"
    email_map_path = output_dir / "email_company_map.csv"

    # 最新のマスタインデックスで再構築
    fresh_index = build_master_index(master_rows)
    email_count = regenerate_email_map(log_path, fresh_index, email_map_path)
    print(f"email_company_map.csv 出力: {email_count} 件")
    print(f"  file:///{str(email_map_path).replace(chr(92), '/')}")

    # 7. サマリ
    with_permit = sum(1 for r in master_rows if r.get("permit_number"))
    corp_count = sum(1 for r in master_rows if r.get("corporation_type") == "CORPORATION")
    sole_count = sum(1 for r in master_rows if r.get("corporation_type") == "SOLE_PROPRIETOR")

    print(f"\n=== Summary ===")
    print(f"  既存会社:       {len(existing_rows)} 社")
    print(f"  更新:           {updated_count} 社")
    print(f"  新規追加:       {new_count} 社")
    print(f"  合計:           {len(master_rows)} 社")
    print(f"  許可証あり:     {with_permit} 社")
    print(f"  法人:           {corp_count} 社")
    print(f"  個人事業主:     {sole_count} 社")
    print(f"  メールマップ:   {email_count} 件")


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
    generate(csv_paths, staging_dir)


if __name__ == "__main__":
    main()
