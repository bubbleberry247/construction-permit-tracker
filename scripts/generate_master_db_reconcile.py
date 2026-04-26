"""
マスタ (145社_許可一覧.xlsx) と DB (companies テーブル) の突合せレポート生成。

出力:
    output/master_db_reconcile_YYYYMMDD_HHMMSS.xlsx

シート構成:
    - サマリー
    - マッチ済み (正規化名で一致)
    - マスタにあるが DB にない
    - DB にあるがマスタにない (派生エントリの可能性)

正規化:
    - 株式会社/(株)/㈱ 等を統一
    - 全角/半角スペース除去
    - 営業所/支店等の suffix を除去した上で再マッチ
"""
from __future__ import annotations

import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "permit_tracker.db"
OUTPUT = PROJECT_ROOT / "output"
MASTER_XLSX = Path(r"C:/Users/owner/Desktop/145社_許可一覧.xlsx")

# 正規化に使う suffix（営業所/支店 等の除去用）
BRANCH_SUFFIXES = ["刈谷統括営業所", "営業所", "支店", "支社", "事業所", "工場"]
COMPANY_SUFFIX_REGEX = re.compile(r"(株式会社|有限会社|合同会社|合資会社|合名会社|\(株\)|（株）|㈱|\(有\)|（有）|㈲)")

# カタカナ大小統一（マスタ「シヤ」vs DB「シャ」の表記揺れ吸収）
KATAKANA_SMALL_TO_LARGE = str.maketrans({
    "ァ": "ア", "ィ": "イ", "ゥ": "ウ", "ェ": "エ", "ォ": "オ",
    "ヵ": "カ", "ヶ": "ケ",
    "ッ": "ツ", "ャ": "ヤ", "ュ": "ユ", "ョ": "ヨ", "ヮ": "ワ",
})


def normalize(name: str) -> str:
    """会社名を正規化: NFKC + カタカナ大小統一 + 法人形態除去 + 空白除去"""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", str(name).strip())
    s = s.translate(KATAKANA_SMALL_TO_LARGE)
    s = COMPANY_SUFFIX_REGEX.sub("", s)
    s = re.sub(r"\s+", "", s)
    return s


def normalize_strip_branch(name: str) -> str:
    """正規化 + 営業所等の suffix 除去（より緩いマッチ用）"""
    s = normalize(name)
    for suf in BRANCH_SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    return s


def load_master() -> list[dict]:
    wb = openpyxl.load_workbook(MASTER_XLSX, data_only=True)
    ws = wb["145社許可一覧"]
    headers = [c.value for c in ws[1]]
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0]:
            rec = {h: v for h, v in zip(headers, row)}
            rec["_name"] = str(row[0]).strip()
            rec["_norm"] = normalize(rec["_name"])
            rec["_norm_strip"] = normalize_strip_branch(rec["_name"])
            rows.append(rec)
    return rows


def load_db() -> list[dict]:
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    rows = []
    for r in conn.execute(
        "SELECT company_id, official_name, status, created_at FROM companies"
    ).fetchall():
        rows.append({
            "company_id": r["company_id"],
            "official_name": r["official_name"],
            "status": r["status"],
            "created_at": r["created_at"],
            "_norm": normalize(r["official_name"]),
            "_norm_strip": normalize_strip_branch(r["official_name"]),
        })
    return rows


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    master = load_master()
    db = load_db()

    # マスタの normalize_strip → DB エントリへの逆引き
    db_by_norm: dict[str, list[dict]] = {}
    db_by_norm_strip: dict[str, list[dict]] = {}
    for d in db:
        db_by_norm.setdefault(d["_norm"], []).append(d)
        db_by_norm_strip.setdefault(d["_norm_strip"], []).append(d)

    matched = []           # (master_rec, db_rec, match_level)
    master_unmatched = []  # マスタにあるが DB にない
    db_used: set[str] = set()  # 使用済み DB company_id

    for m in master:
        # 厳密マッチ
        cands = db_by_norm.get(m["_norm"], [])
        cands = [c for c in cands if c["company_id"] not in db_used]
        if cands:
            chosen = cands[0]
            matched.append((m, chosen, "完全一致"))
            db_used.add(chosen["company_id"])
            continue
        # ゆるマッチ（営業所除去）
        cands = db_by_norm_strip.get(m["_norm_strip"], [])
        cands = [c for c in cands if c["company_id"] not in db_used]
        if cands:
            chosen = cands[0]
            matched.append((m, chosen, "近似一致(営業所差)"))
            db_used.add(chosen["company_id"])
            continue
        master_unmatched.append(m)

    # DB にあってマスタにない
    db_unmatched = [d for d in db if d["company_id"] not in db_used]

    # Excel 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT / f"master_db_reconcile_{ts}.xlsx"
    wb = openpyxl.Workbook()

    # ---------- サマリー ----------
    ws = wb.active
    ws.title = "サマリー"
    bold = Font(bold=True)
    fill_h = PatternFill("solid", fgColor="DDEBF7")
    ws["A1"] = "突合せレポート"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A3"] = "生成日時"
    ws["B3"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ws["A5"] = "件数集計"
    ws["A5"].font = bold
    summary = [
        ("マスタ (145社_許可一覧)", len(master)),
        ("DB 全社 (companies)", len(db)),
        ("　うち ACTIVE", sum(1 for d in db if d["status"] == "ACTIVE")),
        ("　うち INACTIVE", sum(1 for d in db if d["status"] == "INACTIVE")),
        ("　うち MERGED", sum(1 for d in db if d["status"] == "MERGED")),
        ("マッチ済み (完全＋近似)", len(matched)),
        ("　うち 完全一致", sum(1 for _, _, lvl in matched if lvl == "完全一致")),
        ("　うち 近似一致(営業所差)", sum(1 for _, _, lvl in matched if lvl != "完全一致")),
        ("マスタにあるが DB にない", len(master_unmatched)),
        ("DB にあるがマスタにない", len(db_unmatched)),
    ]
    for i, (label, val) in enumerate(summary, start=6):
        ws.cell(row=i, column=1).value = label
        ws.cell(row=i, column=2).value = val
    ws.column_dimensions["A"].width = 38
    ws.column_dimensions["B"].width = 12

    # ---------- マッチ済み ----------
    ws2 = wb.create_sheet("マッチ済み")
    headers = ["#", "マッチ", "マスタ名", "DB company_id", "DB official_name", "DB status"]
    for i, h in enumerate(headers, start=1):
        c = ws2.cell(row=1, column=i)
        c.value = h
        c.font = bold
        c.fill = fill_h
    for i, (m, d, lvl) in enumerate(matched, start=2):
        ws2.cell(row=i, column=1).value = i - 1
        ws2.cell(row=i, column=2).value = lvl
        ws2.cell(row=i, column=3).value = m["_name"]
        ws2.cell(row=i, column=4).value = d["company_id"]
        ws2.cell(row=i, column=5).value = d["official_name"]
        ws2.cell(row=i, column=6).value = d["status"]
    for col, w in zip("ABCDEF", [4, 18, 35, 10, 35, 10]):
        ws2.column_dimensions[col].width = w

    # ---------- マスタにあるが DB にない ----------
    ws3 = wb.create_sheet("マスタにあるがDBになし")
    headers = ["#", "マスタ名"]
    for i, h in enumerate(headers, start=1):
        c = ws3.cell(row=1, column=i)
        c.value = h
        c.font = bold
        c.fill = PatternFill("solid", fgColor="FCE4D6")
    for i, m in enumerate(master_unmatched, start=2):
        ws3.cell(row=i, column=1).value = i - 1
        ws3.cell(row=i, column=2).value = m["_name"]
    ws3.column_dimensions["A"].width = 4
    ws3.column_dimensions["B"].width = 50

    # ---------- DB にあるがマスタにない ----------
    ws4 = wb.create_sheet("DBにあるがマスタになし")
    headers = ["#", "DB company_id", "DB official_name", "DB status", "created_at"]
    for i, h in enumerate(headers, start=1):
        c = ws4.cell(row=1, column=i)
        c.value = h
        c.font = bold
        c.fill = PatternFill("solid", fgColor="FFF2CC")
    for i, d in enumerate(db_unmatched, start=2):
        ws4.cell(row=i, column=1).value = i - 1
        ws4.cell(row=i, column=2).value = d["company_id"]
        ws4.cell(row=i, column=3).value = d["official_name"]
        ws4.cell(row=i, column=4).value = d["status"]
        ws4.cell(row=i, column=5).value = d["created_at"]
    for col, w in zip("ABCDE", [4, 12, 40, 10, 22]):
        ws4.column_dimensions[col].width = w

    wb.save(out_path)
    print(f"出力: {out_path}")
    for label, val in summary:
        print(f"  {label}: {val}")


if __name__ == "__main__":
    main()
