"""
マスタ145社主軸の一覧表 v2 を生成（藤田さん納品用 改訂版）。

方針: マスタ145社のみが管理対象。マスタ外の DB レコードは納品物から除外。

Stage 1: DB read-only。表記揺れ吸収のため aggressive マッチングを既定で使用。

シート構成:
    1. マスタ145社受領状況 (145行 + ヘッダ)
    2. サマリー (件数集計)
    ※ DB独自社シートは方針上「除外」のため出力しない (内部参考は突合せレポート参照)

出力先:
    --execute  : output/review_list_v2_YYYYMMDD_HHMMSS.xlsx
    --dry-run  : output/_dryrun/review_list_v2_DRYRUN_YYYYMMDD_HHMMSS.xlsx (既定)
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_master_db_reconcile import (
    DB,
    MASTER_XLSX,
    load_db,
    load_master,
    match_companies,
)

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "output"
DRYRUN_OUTPUT = DEFAULT_OUTPUT / "_dryrun"

REQUIRED_DOCS = [
    "取引申請書", "建設業許可証", "決算書", "工事経歴書",
    "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿",
]

# company_id → 一覧表「備考」列に出す特例メモ
# 例: 三和シャッターは工事経歴書を業務上提出不可と回答 (2026-04-24)
# 将来複数社で発生したら DB テーブル化を検討
COMPANY_NOTES = {
    "C0008": "藤田: 工事経歴書 三和側ポリシーで提出不可 (2026-04-24 確定)",
    "C0060": "藤田: 工事経歴書 お断り、書類不備のまま審査へ",
    "C0029": "藤田: 取引申請書・誓約書 4/10メールに添付あり",
    "C0023": "藤田: 決算書・工事経歴書・取引先一覧 3/28メールに添付あり",
}


def open_readonly(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = 1")
    return conn


def fetch_company_details(conn: sqlite3.Connection, company_id: str) -> dict:
    cur = conn.cursor()
    permit = cur.execute(
        "SELECT p.permit_number, p.permit_authority, p.permit_category, "
        "p.permit_year, p.issue_date, p.expiry_date, "
        "GROUP_CONCAT(pt.trade_name, '、') AS trades "
        "FROM permits p LEFT JOIN permit_trades pt ON pt.permit_id = p.permit_id "
        "WHERE p.company_id=? AND p.current_flag=1 "
        "GROUP BY p.permit_id "
        "ORDER BY p.expiry_date DESC LIMIT 1",
        (company_id,),
    ).fetchone()

    # 主分類 + 副分類の両方を集計（1ページ2タブ対応、migration 005）
    doc_types = set()
    for r in cur.execute(
        "SELECT DISTINCT doc_type_name FROM pages WHERE company_id=?", (company_id,)
    ):
        if r[0]:
            doc_types.add(r[0])
    for r in cur.execute(
        "SELECT DISTINCT doc_type_secondary FROM pages "
        "WHERE company_id=? AND doc_type_secondary IS NOT NULL", (company_id,)
    ):
        if r[0]:
            doc_types.add(r[0])

    # exemption 取得（卸業者で工事経歴書不要 等）
    exempts = {r[0] for r in cur.execute(
        "SELECT document_type FROM company_doc_exemptions "
        "WHERE company_id=? AND exempt_kind IN ('NOT_APPLICABLE','ALTERNATIVE')",
        (company_id,)
    )}

    doc_checks = {}
    for dt in REQUIRED_DOCS:
        if dt in exempts:
            doc_checks[dt] = "—"  # 対象外
        elif dt in doc_types:
            doc_checks[dt] = "○"
        else:
            doc_checks[dt] = "×"

    # 揃い判定: 対象外を分母から除外
    required_for_company = [dt for dt in REQUIRED_DOCS if dt not in exempts]
    has_count = sum(1 for dt in required_for_company if dt in doc_types)
    if not required_for_company:
        approve = "◎"  # 全 exemption (起こりにくいが念のため)
    elif has_count == len(required_for_company):
        approve = "◎"
    elif has_count > 0:
        approve = "△"
    else:
        approve = "×"

    last_recv = cur.execute(
        "SELECT MAX(received_at) FROM inbound_messages WHERE company_id=?", (company_id,)
    ).fetchone()[0]

    return {
        "permit": permit,
        "doc_checks": doc_checks,
        "has_count": has_count,
        "required_count": len(required_for_company),
        "approve": approve,
        "last_recv": last_recv or "",
        "exempts": exempts,
    }


def classify_db_only(d: dict, conn: sqlite3.Connection) -> str:
    if d["status"] == "INACTIVE":
        return "過去取引(INACTIVE)"
    if d["status"] == "MERGED":
        return "統合済(MERGED)"
    cur = conn.cursor()
    pages = cur.execute("SELECT COUNT(*) FROM pages WHERE company_id=?", (d["company_id"],)).fetchone()[0]
    msgs = cur.execute("SELECT COUNT(*) FROM inbound_messages WHERE company_id=?", (d["company_id"],)).fetchone()[0]
    if pages == 0 and msgs == 0:
        return "幽霊レコード候補(自動登録残骸)"
    return "表記揺れ未マッチ候補"


def write_main_sheet(wb, master_rows, conn, matched_map):
    ws = wb.active
    ws.title = "マスタ145社受領状況"

    headers = [
        "#", "会社ID", "マスタ会社名", "DB official_name", "マッチ種別",
        "状態", "行政庁", "般特", "許可年次", "許可番号", "許可日", "有効期限", "建築許可の種類",
        *REQUIRED_DOCS,
        "揃い", "審査可", "最終受信", "備考",
    ]
    ws.append(headers)

    header_fill = PatternFill(start_color="1B3D6F", end_color="1B3D6F", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(1, col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    cnt = {"ready": 0, "partial": 0, "none": 0, "unreg": 0}
    for i, m in enumerate(master_rows, start=1):
        m_name = m["_name"]
        match_info = matched_map.get(m["_master_index"])

        if match_info is None:
            row = [i, "", m_name, "", "未登録", "", "", "", "", "", "", "", ""]
            row.extend(["×"] * len(REQUIRED_DOCS))
            row.extend([f"0/{len(REQUIRED_DOCS)}", "×", "", ""])
            cnt["unreg"] += 1
        else:
            d, level = match_info
            details = fetch_company_details(conn, d["company_id"])
            permit = details["permit"]
            row = [
                i, d["company_id"], m_name, d["official_name"], level,
                d["status"],
                permit["permit_authority"] if permit else "",
                permit["permit_category"] if permit else "",
                permit["permit_year"] if permit else "",
                permit["permit_number"] if permit else "",
                permit["issue_date"] if permit else "",
                permit["expiry_date"] if permit else "",
                permit["trades"] if permit and permit["trades"] else "",
            ]
            row.extend([details["doc_checks"][dt] for dt in REQUIRED_DOCS])
            note = COMPANY_NOTES.get(d["company_id"], "")
            if details["exempts"]:
                ex_note = "対象外: " + "/".join(sorted(details["exempts"]))
                note = (note + " | " + ex_note) if note else ex_note
            row.extend([
                f"{details['has_count']}/{details['required_count']}",
                details["approve"],
                details["last_recv"],
                note,
            ])
            if details["approve"] == "◎":
                cnt["ready"] += 1
            elif details["approve"] == "△":
                cnt["partial"] += 1
            else:
                cnt["none"] += 1
        ws.append(row)

    widths = {
        "#": 5, "会社ID": 9, "マスタ会社名": 30, "DB official_name": 30, "マッチ種別": 18,
        "状態": 10, "行政庁": 14, "般特": 6, "許可年次": 8, "許可番号": 28,
        "許可日": 12, "有効期限": 12, "建築許可の種類": 30,
        "揃い": 7, "審査可": 7, "最終受信": 18, "備考": 14,
    }
    for doc in REQUIRED_DOCS:
        widths[doc] = 10
    for i, h in enumerate(headers, 1):
        col_letter = openpyxl.utils.get_column_letter(i)
        ws.column_dimensions[col_letter].width = widths.get(h, 12)

    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "C2"

    green = PatternFill(start_color="D4EDDA", end_color="D4EDDA", fill_type="solid")
    yellow = PatternFill(start_color="FFF3CD", end_color="FFF3CD", fill_type="solid")
    red = PatternFill(start_color="F8D7DA", end_color="F8D7DA", fill_type="solid")
    grey = PatternFill(start_color="E2E3E5", end_color="E2E3E5", fill_type="solid")
    approve_col = headers.index("審査可") + 1
    note_col = headers.index("備考") + 1
    for r in range(2, ws.max_row + 1):
        val = ws.cell(r, approve_col).value
        match_type = ws.cell(r, 5).value
        if match_type == "未登録":
            ws.cell(r, approve_col).fill = grey
        elif val == "◎":
            ws.cell(r, approve_col).fill = green
        elif val == "△":
            ws.cell(r, approve_col).fill = yellow
        elif val == "×":
            ws.cell(r, approve_col).fill = red

    return cnt


def write_db_only_sheet(wb, db_unmatched, conn):
    ws = wb.create_sheet("DB独自社（参考）")
    headers = ["#", "company_id", "official_name", "status", "created_at", "推定区分"]
    ws.append(headers)

    bold = Font(bold=True)
    fill_h = PatternFill("solid", fgColor="FFF2CC")
    for i, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=i)
        c.font = bold
        c.fill = fill_h
        c.alignment = Alignment(horizontal="center", vertical="center")

    classify_counts: dict[str, int] = {}
    for i, d in enumerate(db_unmatched, start=2):
        cls = classify_db_only(d, conn)
        classify_counts[cls] = classify_counts.get(cls, 0) + 1
        ws.cell(row=i, column=1).value = i - 1
        ws.cell(row=i, column=2).value = d["company_id"]
        ws.cell(row=i, column=3).value = d["official_name"]
        ws.cell(row=i, column=4).value = d["status"]
        ws.cell(row=i, column=5).value = d["created_at"]
        ws.cell(row=i, column=6).value = cls

    for col, w in zip("ABCDEF", [4, 12, 36, 10, 22, 30]):
        ws.column_dimensions[col].width = w

    return classify_counts


def write_summary_sheet(wb, master_count, matched_count, level_counts, doc_counts, mode):
    ws = wb.create_sheet("サマリー")
    ws["A1"] = "一覧表 v2 サマリー (マスタ145社主軸)"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A3"] = "生成日時"
    ws["B3"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws["A4"] = "マッチモード"
    ws["B4"] = mode

    rows = [
        ("", ""),
        ("【マスタ145社内訳】", ""),
        ("マスタ総数", master_count),
        ("　マッチ済 (合計)", matched_count),
        ("　　うち 完全一致", level_counts.get("完全一致", 0)),
        ("　　うち 近似一致(営業所差)", level_counts.get("近似一致(営業所差)", 0)),
        ("　　うち 近似一致(旧字/新字)", level_counts.get("近似一致(旧字/新字)", 0)),
        ("　　うち 近似一致(部分一致)", level_counts.get("近似一致(部分一致)", 0)),
        ("　未マッチ (当方未登録)", master_count - matched_count),
        ("", ""),
        ("【受領状況 (マスタ145社)】", ""),
        ("　◎ 全書類揃い", doc_counts["ready"]),
        ("　△ 一部受領", doc_counts["partial"]),
        ("　× 未受領 (DB登録あり書類なし)", doc_counts["none"]),
        ("　- 当方未登録", doc_counts["unreg"]),
        ("　※ 合計検算", sum(doc_counts.values())),
        ("", ""),
        ("注: マスタ外の DB レコードは方針上「除外」のため本一覧表には記載しません。", ""),
        ("    詳細は『00_突合せレポート_v2』の『DBにあるがマスタになし』シートをご参照ください。", ""),
    ]

    for i, (label, val) in enumerate(rows, start=5):
        ws.cell(row=i, column=1).value = label
        ws.cell(row=i, column=2).value = val
    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["B"].width = 14


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master-path", default=str(MASTER_XLSX))
    ap.add_argument("--db-path", default=str(DB))
    ap.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--match-mode", choices=["basic", "aggressive"], default="aggressive")
    ap.add_argument("--execute", action="store_true",
                    help="本実行（既定: dry-run）")
    args = ap.parse_args()

    is_execute = args.execute
    is_dryrun = not is_execute

    # P1 反映: 引数のパスを全 IO で使用
    master = load_master(args.master_path)
    db_records = load_db(args.db_path, read_only=True)

    matched, master_unmatched, db_unmatched = match_companies(
        master, db_records, mode=args.match_mode
    )

    # P1 反映: 同名重複を潰さないよう master_index でマップ
    matched_map = {m["_master_index"]: (d, lvl) for m, d, lvl in matched}
    level_counts: dict[str, int] = {}
    for _, _, lvl in matched:
        level_counts[lvl] = level_counts.get(lvl, 0) + 1

    output_dir = DRYRUN_OUTPUT if is_dryrun else Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # P1 反映: ms 単位 timestamp で同一秒衝突回避
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    fname = f"review_list_v2_{'DRYRUN_' if is_dryrun else ''}{ts}.xlsx"
    out_path = output_dir / fname
    if out_path.exists():
        raise FileExistsError(f"既存ファイル衝突を検知: {out_path}")

    conn = open_readonly(args.db_path)

    wb = openpyxl.Workbook()
    doc_counts = write_main_sheet(wb, master, conn, matched_map)
    # 方針: マスタ外の DB レコードは納品物から除外。DB独自社シートは出力しない
    write_summary_sheet(
        wb, len(master), len(matched), level_counts, doc_counts, args.match_mode,
    )
    wb.save(out_path)
    conn.close()

    print(f"{'[DRY-RUN] ' if is_dryrun else ''}出力: {out_path}")
    print(f"=== マスタ145社主軸 ===")
    print(f"  マスタ総数:        {len(master)}")
    print(f"  マッチ済:          {len(matched)}")
    for lvl, n in level_counts.items():
        print(f"    {lvl}: {n}")
    print(f"  未マッチ(未登録):  {len(master) - len(matched)}")
    print(f"=== 受領状況 ===")
    print(f"  ◎ 全揃い:        {doc_counts['ready']}")
    print(f"  △ 一部:          {doc_counts['partial']}")
    print(f"  × 未受領:        {doc_counts['none']}")
    print(f"  - 当方未登録:    {doc_counts['unreg']}")
    print(f"  検算合計:        {sum(doc_counts.values())}  (期待: {len(master)})")
    print(f"=== DB独自社 (納品物から除外) ===")
    print(f"  DB全社:          {len(db_records)}")
    print(f"  除外社数:        {len(db_unmatched)} (内訳は突合せレポート参照)")

    # P2 反映: 検算失敗で非ゼロ終了
    errors = []
    if sum(doc_counts.values()) != len(master):
        errors.append(f"受領状況合計({sum(doc_counts.values())}) != マスタ総数({len(master)})")
    if len(db_unmatched) != len(db_records) - len(matched):
        errors.append(
            f"DB独自社({len(db_unmatched)}) != DB全社({len(db_records)}) - マッチ済({len(matched)})"
        )
    if errors:
        print("\n!! 検算エラー:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(2)

    if is_dryrun:
        print(f"\n※ dry-run モード。本実行は --execute オプションを付けてください。")


if __name__ == "__main__":
    main()
