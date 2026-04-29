"""
Phase R-1: 藤田会社名 ↔ 当方 company_id マッピング作成 + 永続化。

8 段階正規化で機械突合せ → confidence 判定 (HIGH/MEDIUM/LOW/UNMATCHED)。
HIGH のみ自動採用、MEDIUM/LOW は候補提示で人間承認 (Phase R-2 ワークシート 1)。
確定済 mapping は fujita_mapping テーブルに永続化、次回返送時に再利用。

Usage:
  python scripts/build_fujita_mapping.py <fujita_xlsx>
  python scripts/build_fujita_mapping.py <fujita_xlsx> --apply-permitted-mapping (確定 mapping を再利用)
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
V2_XLSX = PROJECT / "output" / "FDE_MANAGED" / "00_一覧表_v2_20260427.xlsx"

# 共通単語 (社名候補から除外する補助単語)
COMMON_WORDS = {"会社", "工業", "工務店", "建設", "建築", "設備"}


# 8 段階正規化
def n1_strip_corp(s: str) -> str:
    return re.sub(
        r"\(株\)|\(有\)|\(合\)|株式会社|有限会社|合同会社|合資会社|合名会社", "", s
    )


def n2_strip_space(s: str) -> str:
    return re.sub(r"[\s　]", "", s)


def n3_normalize_width(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def n4_kata_hira(s: str) -> str:
    out = []
    for ch in s:
        cp = ord(ch)
        if 0x30A1 <= cp <= 0x30F6:
            out.append(chr(cp - 0x60))
        else:
            out.append(ch)
    return "".join(out)


def n5_long_sound(s: str) -> str:
    return s.replace("ー", "").replace("―", "").replace("‐", "")


def n6_yo_tsu_normalize(s: str) -> str:
    return s.replace("ャ", "ヤ").replace("ュ", "ユ").replace("ョ", "ヨ").replace("ッ", "ツ")


def n7_lower(s: str) -> str:
    return s.lower()


def n8_strip_punct(s: str) -> str:
    return re.sub(r"[・,，.\-/\(\)（）\[\]【】「」『』]", "", s)


NORMALIZERS = [n1_strip_corp, n2_strip_space, n3_normalize_width, n4_kata_hira,
                n5_long_sound, n6_yo_tsu_normalize, n7_lower, n8_strip_punct]


def normalize_steps(s: str) -> list[str]:
    """各段階の累積正規化結果を返す (8 個)"""
    if not isinstance(s, str):
        return [""] * len(NORMALIZERS)
    out = []
    cur = s
    for fn in NORMALIZERS:
        cur = fn(cur)
        out.append(cur)
    return out


def fully_normalized(s: str) -> str:
    return normalize_steps(s)[-1]


def load_fujita(xlsx_path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb["継続取引業者リスト"]
    rows = []
    for r in range(3, ws.max_row + 1):
        name = ws.cell(r, 2).value
        if not isinstance(name, str) or name in ("会社名", "-"):
            continue
        rows.append({
            "fujita_row_id": r,
            "fujita_name": name,
            "fujita_status": ws.cell(r, 44).value,
            "permit_number": ws.cell(r, 7).value,
            "permit_authority": ws.cell(r, 10).value,
            "expiry": ws.cell(r, 40).value,
        })
    return rows


def load_v2() -> list[dict]:
    wb = openpyxl.load_workbook(V2_XLSX, data_only=True)
    ws = wb["マスタ145社受領状況"]
    rows = []
    for r in range(2, ws.max_row + 1):
        cid = ws.cell(r, 2).value
        if not cid:
            continue
        rows.append({
            "company_id": cid,
            "master_name": ws.cell(r, 3).value,
            "db_name": ws.cell(r, 4).value,
            "state": ws.cell(r, 6).value,
        })
    return rows


def match_one(fnm: str, v2_index: dict[str, dict]) -> tuple[str, str, dict | None]:
    """
    return (confidence, method, matched_v2_row)
    confidence: HIGH/MEDIUM/LOW/UNMATCHED
    """
    steps = normalize_steps(fnm)
    # 1. 完全一致 (前処理なし) → HIGH
    if fnm in v2_index:
        return "HIGH", "exact", v2_index[fnm]
    # 2. 全角半角・空白統一のみ (n1-n3) → HIGH
    n3 = steps[2]
    n3_idx = {fully_normalized(""): None}  # placeholder
    # Build temp index lazily
    # 3. 8 段階フル正規化 → HIGH/MEDIUM 判定
    full = steps[-1]
    cands = []
    for k_full, v in v2_index.items():
        if v.get("_full") == full:
            cands.append(v)
    if len(cands) == 1:
        return "HIGH", "normalized_full", cands[0]
    if len(cands) > 1:
        return "MEDIUM", "normalized_ambiguous", cands[0]
    # 4. 部分一致 (LOW)
    cands = []
    for v in v2_index.values():
        v_full = v["_full"]
        if not v_full or not full:
            continue
        if v_full in full or full in v_full:
            cands.append(v)
    if cands:
        return "LOW", "substring", cands[0]
    return "UNMATCHED", "none", None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fujita_xlsx", type=Path)
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = ap.parse_args()

    if not args.fujita_xlsx.exists():
        print(f"ERROR: {args.fujita_xlsx} not found", file=sys.stderr)
        sys.exit(1)

    fujita_ts = args.ts
    print(f"Loading 藤田: {args.fujita_xlsx}")
    fujita = load_fujita(args.fujita_xlsx)
    print(f"  -> {len(fujita)} 社")

    print(f"Loading 当方 v2: {V2_XLSX}")
    v2 = load_v2()
    print(f"  -> {len(v2)} 社")

    # v2 の正規化済 index (master_name and db_name)
    v2_index = {}
    for v in v2:
        for nm in (v.get("master_name"), v.get("db_name")):
            if isinstance(nm, str):
                v_copy = dict(v)
                v_copy["_full"] = fully_normalized(nm)
                v_copy["_match_name"] = nm
                v2_index.setdefault(nm, v_copy)

    # 既存 mapping 読み込み (再利用)
    conn = sqlite3.connect(DB_PATH)
    existing = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT fujita_name, company_id FROM fujita_mapping WHERE confirmed_at IS NOT NULL"
        )
    }
    print(f"  既存確定 mapping: {len(existing)} 件 (再利用対象)")

    results = []
    stats = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "UNMATCHED": 0, "REUSED": 0}
    for f in fujita:
        fnm = f["fujita_name"]
        if fnm in existing:
            cid = existing[fnm]
            results.append({**f, "company_id": cid, "confidence": "HIGH",
                            "match_method": "reused_confirmed", "matched_name": ""})
            stats["REUSED"] += 1
            continue
        conf, method, matched = match_one(fnm, v2_index)
        cid = matched["company_id"] if matched else None
        results.append({
            **f,
            "company_id": cid,
            "confidence": conf,
            "match_method": method,
            "matched_name": matched.get("_match_name", "") if matched else "",
            "matched_state": matched.get("state", "") if matched else "",
        })
        stats[conf] += 1

    print()
    print("=== Match summary ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # DB に書き込み (UPSERT 風)
    written = 0
    for r in results:
        existing_row = conn.execute(
            "SELECT mapping_id FROM fujita_mapping WHERE fujita_row_id=? AND fujita_file_ts=?",
            (r["fujita_row_id"], fujita_ts),
        ).fetchone()
        if existing_row:
            conn.execute(
                "UPDATE fujita_mapping SET fujita_name=?, company_id=?, confidence=?, match_method=? WHERE mapping_id=?",
                (r["fujita_name"], r["company_id"], r["confidence"], r["match_method"], existing_row[0]),
            )
        else:
            conn.execute(
                "INSERT INTO fujita_mapping (fujita_row_id, fujita_name, company_id, confidence, match_method, fujita_file_ts) VALUES (?, ?, ?, ?, ?, ?)",
                (r["fujita_row_id"], r["fujita_name"], r["company_id"], r["confidence"], r["match_method"], fujita_ts),
            )
        written += 1
    conn.commit()
    print(f"  fujita_mapping に書き込み: {written} 件")
    conn.close()

    # Excel 出力 (4 シート)
    out_path = PROJECT / "output" / "FDE_MANAGED" / f"05_藤田マッピング_{fujita_ts}.xlsx"
    out_wb = openpyxl.Workbook()
    out_wb.remove(out_wb.active)

    color = {
        "HIGH": "C6EFCE", "MEDIUM": "FFEB9C", "LOW": "FFC7CE", "UNMATCHED": "D9D9D9",
        "REUSED": "DDEBF7",
    }

    headers = ["fujita_row_id", "fujita_name", "company_id", "matched_name",
                "confidence", "match_method", "fujita_status", "matched_state",
                "permit_number", "担当者判定 (同一/別社/新規候補/保留)", "コメント"]

    for sheet_name, target_conf in [
        ("1_HIGH_自動確定", "HIGH"),
        ("2_MEDIUM_要確認", "MEDIUM"),
        ("3_LOW_要確認", "LOW"),
        ("4_UNMATCHED_新規候補", "UNMATCHED"),
    ]:
        ws = out_wb.create_sheet(sheet_name)
        for c, h in enumerate(headers, 1):
            cell = ws.cell(1, c, h)
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor=color.get(target_conf, "FFFFFF"))
        rows_for_sheet = [r for r in results if r["confidence"] == target_conf]
        for i, r in enumerate(rows_for_sheet, 2):
            ws.cell(i, 1, r["fujita_row_id"])
            ws.cell(i, 2, r["fujita_name"])
            ws.cell(i, 3, r["company_id"] or "")
            ws.cell(i, 4, r.get("matched_name", ""))
            ws.cell(i, 5, r["confidence"])
            ws.cell(i, 6, r["match_method"])
            ws.cell(i, 7, r.get("fujita_status") or "")
            ws.cell(i, 8, r.get("matched_state") or "")
            ws.cell(i, 9, r.get("permit_number") or "")
            ws.cell(i, 10, "")  # 担当者編集欄
            ws.cell(i, 11, "")
        # Column widths
        widths = [12, 30, 10, 30, 10, 18, 12, 10, 14, 26, 30]
        for c, w in enumerate(widths, 1):
            ws.column_dimensions[openpyxl.utils.get_column_letter(c)].width = w
        ws.freeze_panes = "A2"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_wb.save(out_path)
    print()
    print(f"=== Output ===")
    print(f"  {out_path.relative_to(PROJECT)}")


if __name__ == "__main__":
    main()
