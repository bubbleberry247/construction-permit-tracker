"""
納品用 index.csv / company_master.csv / doc_type_master.csv を生成する。

前提: scripts/build_delivery_folder.py --execute を実行済で
      output/FDE_MANAGED/10_originals/ に整理済 PDF が配置されている。

出力:
  output/FDE_MANAGED/00_index/
    index.csv              会社×書類×ファイルの台帳（UTF-8 BOM）
    company_master.csv     142 社マスタ + 建設業フラグ
    doc_type_master.csv    9 書類マスタ

index.csv 列:
  record_id, company_code, company_name, doc_type, doc_type_id,
  original_filename, rel_path, sha256,
  page_from, page_to, received_date, retention_years, retention_until,
  is_bundle, note

Usage:
  python scripts/build_index_csv.py
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
CONFIG = PROJECT / "config" / "required_docs.json"
OUTPUT_ROOT = PROJECT / "output" / "FDE_MANAGED"
ORIGINALS_DIR = OUTPUT_ROOT / "10_originals"
INDEX_DIR = OUTPUT_ROOT / "00_index"

REQUIRED_DOCS = json.loads(CONFIG.read_text(encoding="utf-8"))


def load_manifest() -> dict[str, str]:
    """manifest_sha256.txt → {rel_path: sha256}"""
    m: dict[str, str] = {}
    manifest = OUTPUT_ROOT / "manifest_sha256.txt"
    if not manifest.exists():
        print(f"WARN: manifest_sha256.txt が無いので再計算します", file=sys.stderr)
        return m
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split("  ", 1)
        if len(parts) == 2:
            m[parts[1]] = parts[0]
    return m


def calc_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def doc_type_by_folder() -> dict[str, dict]:
    """フォルダ名 → required_docs.json エントリ"""
    m = {}
    for d in REQUIRED_DOCS:
        prefix = d["order"] // 10
        folder = f"{prefix:02d}_{d['display']}"
        m[folder] = d
    return m


def classify_doc_from_path(rel_path: str) -> tuple[str, str, int, bool]:
    """
    rel_path: "10_originals/C0008_xxx/01_建設業許可証/2026/file.pdf"
    → (doc_type_id, doc_type_display, retention_years, is_bundle)
    """
    parts = rel_path.replace("\\", "/").split("/")
    # parts[0]=10_originals, parts[1]=C0008_xxx, parts[2]=<folder>, parts[3]=<year or filename>
    if len(parts) < 4:
        return ("unknown", "不明", 5, True)
    type_folder = parts[2]
    if type_folder == "99_受領バンドル":
        return ("bundle", "受領バンドル", 10, True)
    dt_map = doc_type_by_folder()
    d = dt_map.get(type_folder)
    if d is None:
        return ("unknown", type_folder, 5, False)
    return (d["id"], d["display"], d.get("retention_years", 5), False)


def extract_received_date_from_filename(filename: str) -> str:
    """20260424_101845_xxx.pdf → 2026-04-24"""
    m = re.match(r"^(\d{4})(\d{2})(\d{2})_", filename)
    if not m:
        return ""
    y, mo, d = m.groups()
    try:
        date(int(y), int(mo), int(d))
        return f"{y}-{mo}-{d}"
    except ValueError:
        return ""


def get_page_info(conn: sqlite3.Connection, company_id: str, file_name: str) -> tuple[int, int]:
    """file_name の page_from / page_to（存在する最小・最大 page_no）"""
    rows = conn.execute(
        "SELECT MIN(page_no), MAX(page_no) FROM pages WHERE company_id=? AND file_name=?",
        (company_id, file_name),
    ).fetchone()
    if not rows or rows[0] is None:
        return (0, 0)
    return (rows[0], rows[1])


def doc_type_alias_to_id() -> dict[str, str]:
    """doc_type_name 文字列 → required_docs.json id のマップ（aliases 含む）"""
    m: dict[str, str] = {}
    for d in REQUIRED_DOCS:
        m[d["display"]] = d["id"]
        for a in d.get("aliases", []):
            m[a] = d["id"]
    # OCR 出力の表記揺れ
    m.setdefault("取引申請書", "transaction_terms")
    return m


def get_bundle_segments(
    conn: sqlite3.Connection, company_id: str, file_name: str
) -> list[dict]:
    """bundle PDF のページを doc_type 別にグループ化して返す。

    連続する同 doc_type ページを 1 セグメント (page_from..page_to) とする。
    各 doc_type の **最大セグメント** のみ返す（同 doc_type が複数箇所に分散している
    場合の代表ページを 1 つに絞る）。
    """
    rows = conn.execute(
        "SELECT page_no, doc_type_name FROM pages "
        "WHERE company_id=? AND file_name=? ORDER BY page_no",
        (company_id, file_name),
    ).fetchall()
    if not rows:
        return []

    alias_map = doc_type_alias_to_id()
    # 連続区間に分割
    segments: list[dict] = []
    cur_dt = None
    cur_from = 0
    cur_to = 0
    for r in rows:
        dt = r["doc_type_name"] or ""
        pno = r["page_no"]
        if dt != cur_dt:
            if cur_dt is not None:
                segments.append({"doc_type": cur_dt, "page_from": cur_from, "page_to": cur_to})
            cur_dt = dt
            cur_from = pno
            cur_to = pno
        else:
            cur_to = pno
    if cur_dt is not None:
        segments.append({"doc_type": cur_dt, "page_from": cur_from, "page_to": cur_to})

    # required_docs にマップできるもののみ採用 + doc_type 単位で最長を残す
    best: dict[str, dict] = {}
    for s in segments:
        dt_id = alias_map.get(s["doc_type"])
        if not dt_id:
            continue
        s["doc_type_id"] = dt_id
        s["span"] = s["page_to"] - s["page_from"] + 1
        prev = best.get(dt_id)
        if prev is None or s["span"] > prev["span"]:
            best[dt_id] = s
    return list(best.values())


def main():
    if not ORIGINALS_DIR.exists():
        print(f"ERROR: {ORIGINALS_DIR} が存在しません。先に build_delivery_folder.py --execute を実行してください。",
              file=sys.stderr)
        sys.exit(1)

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # === company_master.csv ===
    companies = conn.execute("""
        SELECT c.company_id, c.official_name, c.corporation_type, c.status,
               CASE WHEN EXISTS (
                 SELECT 1 FROM permits p WHERE p.company_id = c.company_id AND p.current_flag = 1
               ) THEN 1 ELSE 0 END AS has_construction_permit
        FROM companies c
        ORDER BY c.company_id
    """).fetchall()
    cm_path = INDEX_DIR / "company_master.csv"
    with cm_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "company_name", "corporation_type", "status", "has_construction_permit"])
        for r in companies:
            w.writerow([r["company_id"], r["official_name"], r["corporation_type"] or "",
                        r["status"] or "", r["has_construction_permit"]])
    print(f"✓ {cm_path.name}: {len(companies)} 社")

    # === doc_type_master.csv ===
    dtm_path = INDEX_DIR / "doc_type_master.csv"
    with dtm_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["doc_type_id", "order", "folder", "display", "applies_to", "retention_years"])
        for d in REQUIRED_DOCS:
            prefix = d["order"] // 10
            folder = f"{prefix:02d}_{d['display']}"
            w.writerow([d["id"], d["order"], folder, d["display"],
                        d["applies_to"], d.get("retention_years", 5)])
    print(f"✓ {dtm_path.name}: {len(REQUIRED_DOCS)} 書類")

    # === index.csv ===
    index_rows = []
    record_id_counter = 0
    for company_folder in sorted(ORIGINALS_DIR.iterdir()):
        if not company_folder.is_dir():
            continue
        m = re.match(r"^(C\d{4})_(.+)$", company_folder.name)
        if not m:
            continue
        cid, cname = m.group(1), m.group(2)

        # 全 PDF を列挙
        for pdf in sorted(company_folder.rglob("*.pdf")):
            rel_path = str(pdf.relative_to(OUTPUT_ROOT)).replace("\\", "/")
            doc_type_id, display, retention_years, is_bundle = classify_doc_from_path(rel_path)

            # sha256（manifest 優先、無ければ再計算）
            sha = manifest.get(rel_path) or calc_sha256(pdf)

            # ページ数（元の file_name で DB 検索）
            page_from, page_to = get_page_info(conn, cid, pdf.name)

            received = extract_received_date_from_filename(pdf.name)

            # retention_until
            retention_until = ""
            if received:
                try:
                    y, mo, d = received.split("-")
                    retention_until = f"{int(y) + retention_years}-{mo}-{d}"
                except ValueError:
                    pass

            record_id_counter += 1
            index_rows.append({
                "record_id": f"R{record_id_counter:05d}",
                "company_code": cid,
                "company_name": cname,
                "doc_type_id": doc_type_id,
                "doc_type": display,
                "original_filename": pdf.name,
                "rel_path": rel_path,
                "sha256": sha,
                "page_from": page_from,
                "page_to": page_to,
                "received_date": received,
                "retention_years": retention_years,
                "retention_until": retention_until,
                "is_bundle": 1 if is_bundle else 0,
                "note": "",
            })

            # bundle PDF の場合、内包書類を追加レコードとして展開
            if doc_type_id == "bundle":
                segments = get_bundle_segments(conn, cid, pdf.name)
                for seg in segments:
                    seg_display = seg["doc_type"]
                    seg_retention = 5
                    for d in REQUIRED_DOCS:
                        if d["id"] == seg["doc_type_id"]:
                            seg_display = d["display"]
                            seg_retention = d.get("retention_years", 5)
                            break
                    seg_retention_until = ""
                    if received:
                        try:
                            y, mo, dd = received.split("-")
                            seg_retention_until = f"{int(y) + seg_retention}-{mo}-{dd}"
                        except ValueError:
                            pass
                    record_id_counter += 1
                    index_rows.append({
                        "record_id": f"R{record_id_counter:05d}",
                        "company_code": cid,
                        "company_name": cname,
                        "doc_type_id": seg["doc_type_id"],
                        "doc_type": seg_display,
                        "original_filename": pdf.name,
                        "rel_path": rel_path,
                        "sha256": sha,
                        "page_from": seg["page_from"],
                        "page_to": seg["page_to"],
                        "received_date": received,
                        "retention_years": seg_retention,
                        "retention_until": seg_retention_until,
                        "is_bundle": 1,
                        "note": f"in_bundle:{pdf.name}",
                    })

    idx_path = INDEX_DIR / "index.csv"
    with idx_path.open("w", encoding="utf-8-sig", newline="") as f:
        headers = ["record_id", "company_code", "company_name", "doc_type_id", "doc_type",
                   "original_filename", "rel_path", "sha256",
                   "page_from", "page_to", "received_date",
                   "retention_years", "retention_until",
                   "is_bundle", "note"]
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(index_rows)
    print(f"✓ {idx_path.name}: {len(index_rows)} レコード")

    # 集計レポート
    by_type: dict[str, int] = defaultdict(int)
    for r in index_rows:
        by_type[r["doc_type"]] += 1
    print("\n=== 書類種別別レコード数 ===")
    for k, v in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
