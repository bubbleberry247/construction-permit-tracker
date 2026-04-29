"""
Phase R-A: マスタ差分究明レポート (read-only)。

藤田さんマスタと当方マスタの件数差・会社単位の対応関係を全件可視化する。
読み取り専用、DB / xlsx は変更しない。

出力: output/FDE_MANAGED/09_master_diff_<TS>.xlsx
シート構成:
  1_サマリー            : 件数集計、突合せクロス表
  2_藤田のみ            : 藤田にあって当方にない (基本「追加」検討対象)
  3_当方のみ            : 当方にあって藤田にない (INACTIVE化検討対象)
  4_表記揺れペア        : 自動マッチした 43 社の同定確認
  5_藤田未回収・対象外  : 藤田の「未回収・対象外リスト」(48 社) と当方の対応関係

Usage:
  python scripts/master_diff_report.py <fujita_xlsx>
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
from openpyxl.utils import get_column_letter

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
V2_XLSX = PROJECT / "output" / "FDE_MANAGED" / "00_一覧表_v2_20260427.xlsx"

DOC_COLS = ["取引申請書", "建設業許可証", "決算書", "工事経歴書",
             "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]


def normalize_8(s: str) -> str:
    """Phase R-1 と同じ 8 段階正規化"""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"\(株\)|\(有\)|\(合\)|株式会社|有限会社|合同会社|合資会社|合名会社", "", s)
    s = re.sub(r"[\s　]", "", s)
    s = unicodedata.normalize("NFKC", s)
    s = "".join(chr(ord(c) - 0x60) if 0x30A1 <= ord(c) <= 0x30F6 else c for c in s)
    s = s.replace("ー", "").replace("―", "").replace("‐", "")
    s = s.replace("ャ", "ヤ").replace("ュ", "ユ").replace("ョ", "ヨ").replace("ッ", "ツ")
    s = s.lower()
    s = re.sub(r"[・,，.\-/\(\)（）\[\]【】「」『』]", "", s)
    return s


def load_fujita_keizoku(xlsx: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx, data_only=True)
    ws = wb["継続取引業者リスト"]
    rows = []
    for r in range(3, ws.max_row + 1):
        nm = ws.cell(r, 2).value
        if not isinstance(nm, str) or nm in ("会社名", "-"):
            continue
        rows.append({
            "fujita_row_id": r,
            "fujita_name": nm,
            "fujita_status": ws.cell(r, 44).value,
            "permit_number": ws.cell(r, 7).value,
            "permit_authority": ws.cell(r, 10).value,
            "expiry": ws.cell(r, 40).value,
            "memo": ws.cell(r, 48).value,
            "norm": normalize_8(nm),
        })
    return rows


def load_fujita_taishougai(xlsx: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx, data_only=True)
    ws = wb["未回収・対象外リスト"]
    rows = []
    for r in range(2, ws.max_row + 1):
        nm = ws.cell(r, 2).value
        if not isinstance(nm, str) or nm in ("会社名",):
            continue
        rows.append({
            "fujita_row_id": r,
            "fujita_name": nm,
            "contact": ws.cell(r, 3).value,
            "norm": normalize_8(nm),
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
        row = {
            "company_id": cid,
            "master_name": ws.cell(r, 3).value,
            "db_name": ws.cell(r, 4).value,
            "match_kind": ws.cell(r, 5).value,
            "state": ws.cell(r, 6).value,
            "expiry": ws.cell(r, 12).value,
            "soroi": ws.cell(r, 22).value,
        }
        for i, dc in enumerate(DOC_COLS):
            row[dc] = ws.cell(r, 14 + i).value
        row["norm_master"] = normalize_8(row.get("master_name") or "")
        row["norm_db"] = normalize_8(row.get("db_name") or "")
        rows.append(row)
    return rows


def load_db_companies() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    rows = []
    for r in conn.execute(
        "SELECT company_id, official_name, status, permit_number, permit_authority "
        "FROM companies"
    ):
        rows.append({
            "company_id": r[0],
            "official_name": r[1],
            "status": r[2],
            "permit_number": r[3],
            "permit_authority": r[4],
            "norm": normalize_8(r[1] or ""),
        })
    conn.close()
    return rows


def load_existing_mapping() -> dict:
    """Phase R-1 で生成した fujita_mapping を取り込み"""
    conn = sqlite3.connect(DB_PATH)
    out = {}
    for row in conn.execute(
        "SELECT fujita_row_id, fujita_name, company_id, confidence, match_method "
        "FROM fujita_mapping WHERE fujita_file_ts=("
        "  SELECT fujita_file_ts FROM fujita_mapping ORDER BY created_at DESC LIMIT 1"
        ")"
    ):
        out[row[0]] = {
            "fujita_row_id": row[0], "fujita_name": row[1],
            "company_id": row[2], "confidence": row[3], "match_method": row[4],
        }
    conn.close()
    return out


def style_header(ws, n_cols: int, color="305496"):
    for c in range(1, n_cols + 1):
        cell = ws.cell(1, c)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor=color)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fujita_xlsx", type=Path)
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = ap.parse_args()

    if not args.fujita_xlsx.exists():
        print(f"ERROR: {args.fujita_xlsx} not found", file=sys.stderr)
        sys.exit(1)

    print(f"=== Phase R-A: マスタ差分究明レポート ===")
    print(f"  TS: {args.ts}")
    print(f"  fujita_xlsx: {args.fujita_xlsx}")

    fujita_keizoku = load_fujita_keizoku(args.fujita_xlsx)
    fujita_taishougai = load_fujita_taishougai(args.fujita_xlsx)
    v2 = load_v2()
    db_companies = load_db_companies()
    mapping = load_existing_mapping()

    print(f"\n  藤田 継続取引業者リスト: {len(fujita_keizoku)} 社")
    print(f"  藤田 未回収・対象外:     {len(fujita_taishougai)} 社")
    print(f"  当方 v2 マスタ145社:     {len(v2)} 社")
    print(f"  当方 DB companies:       {len(db_companies)} 社")
    print(f"  既存 mapping (Phase R-1): {len(mapping)} 件")

    # マッチング: fujita -> v2 (mapping を優先、なければ正規化突合せ)
    v2_by_norm = {v["norm_master"]: v for v in v2}
    v2_by_norm_db = {v["norm_db"]: v for v in v2 if v.get("norm_db")}
    mapping_by_fujita_row = mapping
    fujita_with_v2 = []
    for f in fujita_keizoku:
        m = mapping_by_fujita_row.get(f["fujita_row_id"])
        cid = m["company_id"] if m else None
        v2_match = next((v for v in v2 if v["company_id"] == cid), None) if cid else None
        if not v2_match:
            v2_match = v2_by_norm.get(f["norm"]) or v2_by_norm_db.get(f["norm"])
        f["v2_match"] = v2_match
        f["mapping_confidence"] = m["confidence"] if m else "UNMATCHED"
        fujita_with_v2.append(f)

    fujita_only = [f for f in fujita_with_v2 if not f["v2_match"]]
    v2_matched_ids = {f["v2_match"]["company_id"] for f in fujita_with_v2 if f["v2_match"]}
    v2_only = [v for v in v2 if v["company_id"] not in v2_matched_ids]

    print(f"\n  突合せ結果:")
    print(f"    マッチ (藤田 ∩ 当方): {len(fujita_with_v2) - len(fujita_only)}")
    print(f"    藤田のみ:              {len(fujita_only)}")
    print(f"    当方のみ:              {len(v2_only)}")

    # 出力 wb 構築
    out_wb = openpyxl.Workbook()
    out_wb.remove(out_wb.active)

    # === シート 1: サマリー ===
    ws1 = out_wb.create_sheet("1_サマリー")
    ws1.cell(1, 1, "項目"); ws1.cell(1, 2, "件数"); ws1.cell(1, 3, "備考")
    style_header(ws1, 3)
    summary = [
        ("藤田 継続取引業者リスト 総数", len(fujita_keizoku), "「会社名」ヘッダー行と空行除外後"),
        ("藤田 未回収・対象外リスト 総数", len(fujita_taishougai), "別シート"),
        ("当方 v2 マスタ145社受領状況", len(v2), "company_id 付きの行のみ"),
        ("当方 DB companies テーブル", len(db_companies), "INACTIVE 含む"),
        ("Phase R-1 既存 mapping", len(mapping), "確定済はそのまま採用"),
        ("--- 突合せ結果 ---", "", ""),
        ("マッチ (藤田 ∩ 当方)", len(fujita_with_v2) - len(fujita_only), ""),
        ("藤田のみ (追加検討対象)", len(fujita_only), "シート 2 参照"),
        ("当方のみ (INACTIVE 検討対象)", len(v2_only), "シート 3 参照"),
        ("--- 藤田ステータス内訳 ---", "", ""),
    ]
    from collections import Counter
    fst = Counter(f.get("fujita_status") or "(空)" for f in fujita_keizoku)
    for k, v in fst.most_common():
        summary.append((f"  {k}", v, ""))
    for i, (k, n, note) in enumerate(summary, 2):
        ws1.cell(i, 1, k); ws1.cell(i, 2, n); ws1.cell(i, 3, note)
    ws1.column_dimensions["A"].width = 30
    ws1.column_dimensions["B"].width = 10
    ws1.column_dimensions["C"].width = 50

    # === シート 2: 藤田のみ (追加検討) ===
    ws2 = out_wb.create_sheet("2_藤田のみ")
    headers2 = ["藤田行番号", "藤田会社名", "藤田status", "許可番号", "行政庁",
                 "期限", "memo", "推奨アクション", "ユーザー最終判定 [追加/別名疑い/対象外/保留]",
                 "別名疑いの場合の company_id", "コメント"]
    for c, h in enumerate(headers2, 1):
        ws2.cell(1, c, h)
    style_header(ws2, len(headers2))
    for i, f in enumerate(fujita_only, 2):
        ws2.cell(i, 1, f["fujita_row_id"])
        ws2.cell(i, 2, f["fujita_name"])
        ws2.cell(i, 3, f.get("fujita_status") or "")
        ws2.cell(i, 4, f.get("permit_number") or "")
        ws2.cell(i, 5, f.get("permit_authority") or "")
        ws2.cell(i, 6, str(f.get("expiry") or ""))
        ws2.cell(i, 7, str(f.get("memo") or "")[:100])
        # 推奨アクション (デフォルト「追加」)
        if f.get("fujita_status") == "対象外":
            ws2.cell(i, 8, "対象外")
        elif f.get("fujita_status") == "未提出":
            ws2.cell(i, 8, "追加 (未提出だが管理対象)")
        else:
            ws2.cell(i, 8, "追加")
    widths = [10, 30, 12, 14, 16, 14, 40, 26, 30, 18, 30]
    for c, w in enumerate(widths, 1):
        ws2.column_dimensions[get_column_letter(c)].width = w
    ws2.freeze_panes = "C2"

    # === シート 3: 当方のみ ===
    ws3 = out_wb.create_sheet("3_当方のみ")
    headers3 = ["company_id", "master_name", "db_name", "match_kind", "state",
                 "受領揃い", "推奨アクション", "ユーザー最終判定 [維持/INACTIVE化/対象外フラグ]",
                 "コメント"]
    for c, h in enumerate(headers3, 1):
        ws3.cell(1, c, h)
    style_header(ws3, len(headers3))
    for i, v in enumerate(v2_only, 2):
        ws3.cell(i, 1, v["company_id"])
        ws3.cell(i, 2, v.get("master_name") or "")
        ws3.cell(i, 3, v.get("db_name") or "")
        ws3.cell(i, 4, v.get("match_kind") or "")
        ws3.cell(i, 5, v.get("state") or "")
        ws3.cell(i, 6, v.get("soroi") or "")
        # デフォルト「維持」(藤田が見落とした可能性)
        ws3.cell(i, 7, "維持 (藤田見落としの可能性)")
    widths3 = [12, 30, 30, 14, 12, 10, 30, 30, 30]
    for c, w in enumerate(widths3, 1):
        ws3.column_dimensions[get_column_letter(c)].width = w
    ws3.freeze_panes = "C2"

    # === シート 4: 表記揺れペア (マッチ済) ===
    ws4 = out_wb.create_sheet("4_表記揺れペア")
    headers4 = ["藤田行番号", "藤田会社名", "当方 master_name", "当方 db_name",
                 "company_id", "mapping confidence", "藤田status", "当方state",
                 "ユーザー確認 [同一確定/別社/保留]", "コメント"]
    for c, h in enumerate(headers4, 1):
        ws4.cell(1, c, h)
    style_header(ws4, len(headers4))
    matched_pairs = [f for f in fujita_with_v2 if f.get("v2_match") and f["fujita_name"] != (f["v2_match"].get("master_name") or "")]
    for i, f in enumerate(matched_pairs, 2):
        v = f["v2_match"]
        ws4.cell(i, 1, f["fujita_row_id"])
        ws4.cell(i, 2, f["fujita_name"])
        ws4.cell(i, 3, v.get("master_name") or "")
        ws4.cell(i, 4, v.get("db_name") or "")
        ws4.cell(i, 5, v["company_id"])
        ws4.cell(i, 6, f.get("mapping_confidence", ""))
        ws4.cell(i, 7, f.get("fujita_status") or "")
        ws4.cell(i, 8, v.get("state") or "")
    widths4 = [10, 30, 30, 30, 12, 14, 12, 12, 26, 30]
    for c, w in enumerate(widths4, 1):
        ws4.column_dimensions[get_column_letter(c)].width = w
    ws4.freeze_panes = "C2"

    # === シート 5: 藤田の未回収・対象外リスト ===
    ws5 = out_wb.create_sheet("5_藤田未回収・対象外")
    headers5 = ["藤田行番号", "藤田会社名", "連絡先", "正規化名", "当方 v2 にあるか",
                 "当方 company_id", "当方 state", "推奨アクション", "コメント"]
    for c, h in enumerate(headers5, 1):
        ws5.cell(1, c, h)
    style_header(ws5, len(headers5))
    for i, f in enumerate(fujita_taishougai, 2):
        v = v2_by_norm.get(f["norm"]) or v2_by_norm_db.get(f["norm"])
        ws5.cell(i, 1, f["fujita_row_id"])
        ws5.cell(i, 2, f["fujita_name"])
        ws5.cell(i, 3, str(f.get("contact") or "")[:60])
        ws5.cell(i, 4, f["norm"])
        ws5.cell(i, 5, "あり" if v else "なし")
        ws5.cell(i, 6, v["company_id"] if v else "")
        ws5.cell(i, 7, v.get("state") if v else "")
        if v and v.get("state") == "ACTIVE":
            ws5.cell(i, 8, "INACTIVE化 検討 (藤田が対象外と認識)")
        else:
            ws5.cell(i, 8, "対象外で確定")
    widths5 = [10, 30, 30, 24, 16, 14, 12, 30, 30]
    for c, w in enumerate(widths5, 1):
        ws5.column_dimensions[get_column_letter(c)].width = w
    ws5.freeze_panes = "C2"

    # 保存
    out_path = PROJECT / "output" / "FDE_MANAGED" / f"09_master_diff_{args.ts}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_wb.save(out_path)
    print(f"\n=== Output ===")
    print(f"  {out_path.relative_to(PROJECT)}")
    print(f"  シート: 1_サマリー / 2_藤田のみ ({len(fujita_only)}) / 3_当方のみ ({len(v2_only)}) / 4_表記揺れペア ({len(matched_pairs)}) / 5_藤田未回収 ({len(fujita_taishougai)})")
    print()
    print("次のアクション:")
    print("  1. シート 2 「藤田のみ」をユーザー (Masaru) がレビュー")
    print("     → 各社に「ユーザー最終判定」を記入 (追加 / 別名疑い / 対象外 / 保留)")
    print("  2. シート 3「当方のみ」もレビュー (デフォルト維持)")
    print("  3. シート 4「表記揺れペア」を確認 (デフォルト同一確定)")
    print("  4. シート 5「藤田未回収」を確認")
    print("  5. レビュー後に Phase R-Backup-2 → Phase R-B (DB 反映) に進む")


if __name__ == "__main__":
    main()
