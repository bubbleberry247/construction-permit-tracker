"""
マスタ (145社_許可一覧.xlsx) と DB (companies テーブル) の突合せレポート生成。

出力:
    output/master_db_reconcile_YYYYMMDD_HHMMSS.xlsx
    (--mode aggressive 時は output/master_db_reconcile_v2_YYYYMMDD_HHMMSS.xlsx)

シート構成:
    - サマリー
    - マッチ済み (正規化名で一致)
    - マスタにあるが DB にない
    - DB にあるがマスタにない (派生エントリの可能性)

正規化:
    - 株式会社/(株)/㈱ 等を統一
    - 全角/半角スペース除去
    - 営業所/支店等の suffix を除去した上で再マッチ
    - aggressive モード: 旧字→新字変換 + subsequence マッチを追加
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
from openpyxl.styles import Font, PatternFill

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "permit_tracker.db"
OUTPUT = PROJECT_ROOT / "output"
MASTER_XLSX = Path(r"C:/Users/owner/Desktop/145社_許可一覧.xlsx")

BRANCH_SUFFIXES = ["刈谷統括営業所", "営業所", "支店", "支社", "事業所", "工場"]
COMPANY_SUFFIX_REGEX = re.compile(r"(株式会社|有限会社|合同会社|合資会社|合名会社|\(株\)|（株）|㈱|\(有\)|（有）|㈲)")

KATAKANA_SMALL_TO_LARGE = str.maketrans({
    "ァ": "ア", "ィ": "イ", "ゥ": "ウ", "ェ": "エ", "ォ": "オ",
    "ヵ": "カ", "ヶ": "ケ",
    "ッ": "ツ", "ャ": "ヤ", "ュ": "ユ", "ョ": "ヨ", "ヮ": "ワ",
})

# 旧字 → 新字（マスタが旧字、DB が新字のケースを吸収）
# 一方向変換: 旧字を新字に寄せる
KYUUJI_TO_SHINJI = str.maketrans({
    "氣": "気", "銕": "鉄", "﨑": "崎", "廣": "広", "國": "国",
    "學": "学", "藝": "芸", "縣": "県", "號": "号", "變": "変",
    "體": "体", "圖": "図", "聲": "声", "樂": "楽", "寫": "写",
    "醫": "医", "經": "経", "證": "証", "應": "応", "邊": "辺",
    "邉": "辺", "渕": "淵", "竈": "竃", "齋": "斎", "齊": "斉",
    "壽": "寿", "靜": "静", "禮": "礼", "繪": "絵", "豐": "豊",
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


def normalize_aggressive(name: str) -> str:
    """normalize + 旧字→新字変換（マスタ旧字↔DB新字の表記揺れ吸収用）"""
    s = normalize(name)
    s = s.translate(KYUUJI_TO_SHINJI)
    return s


def is_subsequence_match(a: str, b: str, min_len: int = 5) -> bool:
    """短い方が長い方の部分配列として完全に出現するか（過マッチ防止のため最低5文字）。

    例: 'ヨコガワシステムケンチク' は 'ヨコガワブリッジシステムケンチク' の部分配列としてマッチ。
    """
    if not a or not b:
        return False
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    if len(short) < min_len:
        return False
    it = iter(long)
    return all(c in it for c in short)


def _enrich(rec: dict, name_key: str) -> dict:
    """rec に _norm/_norm_strip/_norm_aggressive を付与（idempotent）"""
    name = rec[name_key]
    rec.setdefault("_norm", normalize(name))
    rec.setdefault("_norm_strip", normalize_strip_branch(name))
    rec.setdefault("_norm_aggressive", normalize_aggressive(name))
    return rec


def load_master(path: Path | str = MASTER_XLSX) -> list[dict]:
    wb = openpyxl.load_workbook(Path(path), data_only=True)
    ws = wb["145社許可一覧"]
    headers = [c.value for c in ws[1]]
    rows = []
    for idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
        if row[0]:
            rec = {h: v for h, v in zip(headers, row)}
            rec["_name"] = str(row[0]).strip()
            rec["_master_index"] = idx  # 同名重複時の行同定用 (P1反映)
            _enrich(rec, "_name")
            rows.append(rec)
    return rows


def load_db(path: Path | str = DB, read_only: bool = True) -> list[dict]:
    """companies を ORDER BY company_id で取得。既定 read_only=True (P1 反映)。"""
    if read_only:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.execute("PRAGMA query_only = 1")
    else:
        conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    rows = []
    for r in conn.execute(
        "SELECT company_id, official_name, status, created_at FROM companies "
        "ORDER BY company_id"
    ).fetchall():
        rec = {
            "company_id": r["company_id"],
            "official_name": r["official_name"],
            "status": r["status"],
            "created_at": r["created_at"],
        }
        _enrich(rec, "official_name")
        rows.append(rec)
    conn.close()
    return rows


def match_companies(master: list[dict], db: list[dict], mode: str = "basic"):
    """マスタとDBのマッチング。

    Args:
        master: load_master() の戻り値、または同等の dict list (要 _norm/_norm_strip/_norm_aggressive)
        db: load_db() の戻り値、または同等の dict list
        mode: 'basic' (完全+営業所差) / 'aggressive' (basic + 旧字新字 + 部分配列)

    Returns:
        (matched, master_unmatched, db_unmatched)
        matched は list of (master_rec, db_rec, match_level: str)
    """
    if mode not in ("basic", "aggressive"):
        raise ValueError(f"unknown mode: {mode}")

    # 既存挙動互換のため idempotent な enrich を実行
    for m in master:
        _enrich(m, "_name")
    for d in db:
        _enrich(d, "official_name")

    db_by_norm: dict[str, list[dict]] = {}
    db_by_norm_strip: dict[str, list[dict]] = {}
    db_by_norm_agg: dict[str, list[dict]] = {}
    for d in db:
        db_by_norm.setdefault(d["_norm"], []).append(d)
        db_by_norm_strip.setdefault(d["_norm_strip"], []).append(d)
        db_by_norm_agg.setdefault(d["_norm_aggressive"], []).append(d)

    matched: list[tuple] = []
    master_unmatched: list[dict] = []
    db_used: set[str] = set()

    def _take(cands):
        return [c for c in cands if c["company_id"] not in db_used]

    for m in master:
        # Pass 1: 完全一致
        cands = _take(db_by_norm.get(m["_norm"], []))
        if cands:
            chosen = cands[0]
            matched.append((m, chosen, "完全一致"))
            db_used.add(chosen["company_id"])
            continue

        # Pass 2: 近似一致（営業所差）
        cands = _take(db_by_norm_strip.get(m["_norm_strip"], []))
        if cands:
            chosen = cands[0]
            matched.append((m, chosen, "近似一致(営業所差)"))
            db_used.add(chosen["company_id"])
            continue

        if mode == "aggressive":
            # Pass 3: 旧字/新字一致
            cands = _take(db_by_norm_agg.get(m["_norm_aggressive"], []))
            if cands:
                chosen = cands[0]
                matched.append((m, chosen, "近似一致(旧字/新字)"))
                db_used.add(chosen["company_id"])
                continue

            # Pass 4: subsequence マッチ（短い方が長い方に部分配列として含まれる）
            sub_match = None
            for d in db:
                if d["company_id"] in db_used:
                    continue
                if is_subsequence_match(m["_norm_aggressive"], d["_norm_aggressive"], min_len=5):
                    sub_match = d
                    break
            if sub_match:
                matched.append((m, sub_match, "近似一致(部分一致)"))
                db_used.add(sub_match["company_id"])
                continue

        master_unmatched.append(m)

    db_unmatched = [d for d in db if d["company_id"] not in db_used]
    return matched, master_unmatched, db_unmatched


def write_excel(out_path: Path, master, db, matched, master_unmatched, db_unmatched, mode: str) -> dict:
    wb = openpyxl.Workbook()
    bold = Font(bold=True)
    fill_h = PatternFill("solid", fgColor="DDEBF7")

    # ---------- サマリー ----------
    ws = wb.active
    ws.title = "サマリー"
    ws["A1"] = "突合せレポート"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A3"] = "生成日時"
    ws["B3"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws["A4"] = "マッチモード"
    ws["B4"] = mode

    ws["A6"] = "件数集計"
    ws["A6"].font = bold
    summary = [
        ("マスタ (145社_許可一覧)", len(master)),
        ("DB 全社 (companies)", len(db)),
        ("　うち ACTIVE", sum(1 for d in db if d["status"] == "ACTIVE")),
        ("　うち INACTIVE", sum(1 for d in db if d["status"] == "INACTIVE")),
        ("　うち MERGED", sum(1 for d in db if d["status"] == "MERGED")),
        ("マッチ済み (合計)", len(matched)),
        ("　うち 完全一致", sum(1 for _, _, lvl in matched if lvl == "完全一致")),
        ("　うち 近似一致(営業所差)", sum(1 for _, _, lvl in matched if lvl == "近似一致(営業所差)")),
        ("　うち 近似一致(旧字/新字)", sum(1 for _, _, lvl in matched if lvl == "近似一致(旧字/新字)")),
        ("　うち 近似一致(部分一致)", sum(1 for _, _, lvl in matched if lvl == "近似一致(部分一致)")),
        ("マスタにあるが DB にない", len(master_unmatched)),
        ("DB にあるがマスタにない", len(db_unmatched)),
    ]
    for i, (label, val) in enumerate(summary, start=7):
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
    for col, w in zip("ABCDEF", [4, 22, 35, 14, 35, 10]):
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
    for col, w in zip("ABCDE", [4, 14, 40, 10, 22]):
        ws4.column_dimensions[col].width = w

    wb.save(out_path)
    return dict(summary)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["basic", "aggressive"], default="basic",
                    help="basic=既存挙動 (完全+営業所差) / aggressive=旧字新字+部分配列追加")
    ap.add_argument("--output-suffix", default="",
                    help="出力ファイル名のサフィックス (例: v2)")
    args = ap.parse_args()

    OUTPUT.mkdir(parents=True, exist_ok=True)
    master = load_master()
    db = load_db(read_only=True)

    matched, master_unmatched, db_unmatched = match_companies(master, db, mode=args.mode)

    # P1 反映: ms 単位 timestamp で同一秒上書きを防止
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    suffix = f"_{args.output_suffix}" if args.output_suffix else ""
    out_path = OUTPUT / f"master_db_reconcile{suffix}_{ts}.xlsx"
    if out_path.exists():
        raise FileExistsError(f"既存ファイル衝突を検知: {out_path}")
    summary = write_excel(out_path, master, db, matched, master_unmatched, db_unmatched, args.mode)

    print(f"出力: {out_path}")
    for label, val in summary.items():
        print(f"  {label}: {val}")


if __name__ == "__main__":
    main()
