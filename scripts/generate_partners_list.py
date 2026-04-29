"""
継続取引業者リストを生成（参考フォーマット拡張：必要書類列＋ハイパーリンク）。

レイアウト:
  Row 1: 凡例（A1:BC1 全範囲 1 マージ）
  Row 2: ヘッダー（青塗り、55 列）
  Row 3〜: 1 会社 1 行（DB 内 ACTIVE 会社）
  末尾: 未特定取引先（config/unmatched_emails.json）

列構成（55 列）:
  A:    会社名
  B-D:  代表敬称 / 代表者名 / 担当者名
  E:    連絡先(メール・電話)
  F-N:  必要書類 9 列（○受領 / ×未受領 / —対象外、受領セルはハイパーリンク）
  O-R:  許可番号セクション (般/特, 空, 番号末尾, 知事/大臣)
  S-AU: 業種マトリクス 29 列
  AV-AX: 許可満了日 / 更新確認日 / 許可証受領日
  AY:   データ保存場所(フォルダパス) ← ハイパーリンク
  AZ:   現在ステータス
  BA-BC: 書類ステータス / 不足書類 / 直近1年発注実績
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import urllib.parse
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
CONFIG = PROJECT / "config" / "required_docs.json"
UNMATCHED_EMAILS_JSON = PROJECT / "config" / "unmatched_emails.json"
INDEX_CSV = PROJECT / "output" / "FDE_MANAGED" / "00_index" / "index.csv"
OUTPUT_DEFAULT = PROJECT / "output" / f"継続取引業者リスト_{datetime.now().strftime('%Y%m%d')}.xlsx"

REQUIRED_DOCS = json.loads(CONFIG.read_text(encoding="utf-8"))

TRADE_ABBR = [
    ("土", "土木工事業"), ("建", "建築工事業"), ("大", "大工工事業"), ("左", "左官工事業"),
    ("と", "とび・土工工事業"), ("石", "石工事業"), ("屋", "屋根工事業"), ("電", "電気工事業"),
    ("管", "管工事業"), ("タ", "タイル・れんが・ブロック工事業"), ("鋼", "鋼構造物工事業"),
    ("筋", "鉄筋工事業"), ("舗", "舗装工事業"), ("しゅ", "しゅんせつ工事業"), ("板", "板金工事業"),
    ("ガ", "ガラス工事業"), ("塗", "塗装工事業"), ("防", "防水工事業"), ("内", "内装仕上工事業"),
    ("機", "機械器具設置工事業"), ("絶", "熱絶縁工事業"), ("通", "電気通信工事業"),
    ("園", "造園工事業"), ("井", "さく井工事業"), ("具", "建具工事業"), ("水", "水道施設工事業"),
    ("消", "消防施設工事業"), ("清", "清掃施設工事業"), ("解", "解体工事業"),
]

LEGEND_TEXT = (
    "【凡例】 ■必要書類: 〇=受領  ×=未受領  —=対象外（受領セルはハイパーリンクで PDF へ）　"
    "■現在ステータス: 有効 / 期限切れ(赤文字) / 期限90日以内(オレンジ文字) / 未提出・未特定(グレー文字)　"
    "業種略字: 土=土木  建=建築  大=大工  左=左官  と=とび土工  石=石  屋=屋根  電=電気  "
    "管=管  タ=タイル  鋼=鋼構造物  筋=鉄筋  舗=舗装  しゅ=しゅんせつ  板=板金  ガ=ガラス  "
    "塗=塗装  防=防水  内=内装  機=機械器具  絶=熱絶縁  通=電気通信  園=造園  井=さく井  "
    "具=建具  水=水道  消=消防  清=清掃  解=解体"
)

# 列構成
DOC_COLS = [(d["display"], d["id"], d["applies_to"]) for d in REQUIRED_DOCS]  # 9 cols

HEADERS = (
    ["会社名", "代表敬称", "代表者名", "担当者名", "連絡先(メール・電話)"]   # A-E (1-5)
    + [d[0] for d in DOC_COLS]                                                # F-N (6-14)
    + ["建設業許可番号", "", "", "許可区分(知事/大臣)"]                         # O-R (15-18)
    + [abbr for abbr, _ in TRADE_ABBR]                                         # S-AU (19-47)
    + ["許可満了日", "更新確認日", "許可証受領日",                              # AV-AX (48-50)
       "データ保存場所(フォルダパス)", "現在ステータス",                         # AY-AZ (51-52)
       "直近1年発注実績"]                                                       # BA (53)
)
N_COLS = len(HEADERS)
assert N_COLS == 53, f"想定 53 列、実際 {N_COLS}"

# 列インデックス（1-based）
COL_NAME = 1
COL_EMAIL = 5
COL_DOC_START = 6   # F
COL_DOC_END = 14    # N (含む)
COL_PERMIT_GENERAL = 15  # O 般/特
COL_PERMIT_NUMBER = 17   # Q 番号末尾
COL_PERMIT_AUTHORITY = 18  # R 知事/大臣
COL_TRADE_START = 19  # S
COL_TRADE_END = 47    # AU
COL_EXPIRY = 48       # AV
COL_RENEWAL_CHECK = 49  # AW
COL_RECEIVED = 50     # AX
COL_FOLDER = 51       # AY
COL_STATUS = 52       # AZ
COL_ORDERS = 53       # BA

HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
LEGEND_FONT = Font(size=9, color="333333")
GREEN_FONT = Font(color="00873E", bold=True)
ORANGE_FONT = Font(color="E67E22", bold=True)
RED_FONT = Font(color="C00000", bold=True)
GRAY_FONT = Font(color="888888")
LINK_FONT = Font(color="0563C1", underline="single", size=11)
YELLOW_FILL = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
GRAY_FILL = PatternFill(start_color="EFEFEF", end_color="EFEFEF", fill_type="solid")
GREEN_FILL = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
RED_FILL = PatternFill(start_color="FCE8E8", end_color="FCE8E8", fill_type="solid")
CENTER = Alignment(horizontal="center", vertical="center")
LEFT = Alignment(horizontal="left", vertical="center")
THIN_BORDER = Border(
    left=Side(style="thin", color="DDDDDD"), right=Side(style="thin", color="DDDDDD"),
    top=Side(style="thin", color="DDDDDD"), bottom=Side(style="thin", color="DDDDDD"),
)


def to_wareki(iso_date):
    if not iso_date:
        return ""
    try:
        d = date.fromisoformat(iso_date)
    except (ValueError, TypeError):
        return iso_date or ""
    if d >= date(2019, 5, 1):
        return f"R{d.year - 2018}.{d.month}.{d.day}"
    if d >= date(1989, 1, 8):
        return f"H{d.year - 1988}.{d.month}.{d.day}"
    return iso_date


def parse_authority(authority):
    if not authority:
        return ""
    if "国土交通大臣" in authority:
        return "国土交通大臣"
    if "知事" in authority:
        return "都道府県知事"
    return authority


def extract_permit_number_tail(permit_number):
    if not permit_number:
        return ""
    m = re.search(r"第?(\d{3,7})号?$", permit_number)
    return m.group(1) if m else permit_number


def extract_permit_category(permit_number, category):
    if category:
        if "一般" in category or category == "般":
            return "般"
        if "特定" in category or category == "特":
            return "特"
    if permit_number:
        if "般" in permit_number:
            return "般"
        if "特" in permit_number:
            return "特"
    return ""


def determine_status(permit, has_any_doc):
    today = date.today()
    if permit and permit.get("expiry_date"):
        try:
            exp = date.fromisoformat(permit["expiry_date"])
            if exp < today:
                return ("期限切れ", "red")
            if (exp - today).days <= 90:
                return ("期限90日以内", "orange")
            return ("有効", "normal")
        except (ValueError, TypeError):
            pass
    if has_any_doc:
        return ("未特定", "gray")
    return ("未提出", "gray")


def load_companies(conn):
    return [dict(r) for r in conn.execute("""
        SELECT c.company_id, c.official_name, c.corporation_type, c.status
        FROM companies c WHERE c.status='ACTIVE' ORDER BY c.official_name
    """)]


def load_permits(conn):
    permits = {}
    for r in conn.execute("""
        SELECT permit_id, company_id, permit_number, permit_authority,
               permit_category, expiry_date, issue_date
        FROM permits WHERE current_flag=1
        ORDER BY company_id, expiry_date DESC
    """):
        cid = r["company_id"]
        if cid in permits:
            continue
        permits[cid] = dict(r)
        permits[cid]["trades"] = set()
    for r in conn.execute("""
        SELECT pt.trade_name, p.company_id FROM permit_trades pt
        JOIN permits p ON p.permit_id = pt.permit_id WHERE p.current_flag=1
    """):
        if r["company_id"] in permits:
            permits[r["company_id"]]["trades"].add(r["trade_name"])
    return permits


def load_emails(conn):
    emails = {}
    for r in conn.execute("SELECT company_id, email FROM company_emails ORDER BY created_at DESC"):
        emails.setdefault(r["company_id"], r["email"])
    return emails


def load_doc_links(index_csv, conn):
    """
    index.csv → {company_id: {doc_type_id: {rel_path, page_from, is_bundle, original_filename, page_id}}}

    同 doc_type が複数あれば、is_bundle=0（単一書類PDF）を優先、
    page_from が小さい方を優先。
    page_id は pages テーブルから (company_id, file_name=original_filename, page_no=page_from) で解決。
    """
    if not index_csv.exists():
        return {}, {}
    links: dict[str, dict] = defaultdict(dict)
    latest: dict[str, str] = {}
    with index_csv.open("r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            cid = r["company_code"]
            dt_id = r["doc_type_id"]
            if dt_id in ("bundle", "unknown"):
                continue
            rel = r.get("rel_path", "").replace("\\", "/")
            orig = r.get("original_filename", "")
            try:
                pf = int(r.get("page_from") or 0)
            except ValueError:
                pf = 0
            is_bundle = (r.get("is_bundle") or "0") == "1"
            entry = {
                "rel_path": rel, "page_from": pf, "is_bundle": is_bundle,
                "original_filename": orig, "page_id": None,
            }
            existing = links[cid].get(dt_id)
            if existing is None:
                links[cid][dt_id] = entry
            else:
                if existing.get("is_bundle") and not is_bundle:
                    links[cid][dt_id] = entry
            recv = r.get("received_date", "")
            if recv > latest.get(cid, ""):
                latest[cid] = recv

    # page_id 解決
    for cid, dts in links.items():
        for dt_id, e in dts.items():
            if e["page_from"] and e["original_filename"]:
                row = conn.execute(
                    "SELECT page_id FROM pages WHERE company_id=? AND file_name=? AND page_no=?",
                    (cid, e["original_filename"], e["page_from"]),
                ).fetchone()
                if row:
                    e["page_id"] = row["page_id"]
    return dict(links), latest


def load_unmatched_emails():
    if not UNMATCHED_EMAILS_JSON.exists():
        return []
    return json.loads(UNMATCHED_EMAILS_JSON.read_text(encoding="utf-8")).get("emails", [])


def find_company_folder(company_id):
    base = PROJECT / "data" / "originals"
    if not base.exists():
        return None
    for d in base.iterdir():
        if d.is_dir() and d.name.startswith(f"{company_id}_"):
            return d
    return None


def to_link_target_file(rel_path: str, page_no: int | None, prefix: str = "") -> str:
    """ファイル直接リンク。

    prefix: '' なら Excel が FDE_MANAGED/ 内にある前提（10_originals/... が相対）
            'FDE_MANAGED/' なら Excel が output/ 直下にある前提
    """
    p = rel_path.replace("\\", "/")
    if prefix and not p.startswith(prefix):
        p = prefix + p
    if page_no and page_no > 0:
        p += f"#page={page_no}"
    return p


def to_link_target_viewer(company_id: str, page_id: int | None, viewer_base: str) -> str:
    """ビューワー URL（FastAPI）"""
    if page_id:
        return f"{viewer_base}/viewer/{company_id}?focus={page_id}"
    return f"{viewer_base}/viewer/{company_id}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=str(OUTPUT_DEFAULT), help="出力 xlsx パス")
    ap.add_argument("--link-mode", choices=["viewer", "file"], default="viewer",
                    help="ハイパーリンク先: viewer=ビューワー URL、file=ローカル PDF")
    ap.add_argument("--viewer-base", default="http://localhost:8080",
                    help="ビューワーのベース URL（--link-mode=viewer 時のみ）")
    ap.add_argument("--link-prefix", default=None,
                    help="--link-mode=file 時のパスプレフィックス。"
                         "未指定時は --output が FDE_MANAGED/ 配下なら空、それ以外は 'FDE_MANAGED/'")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    companies = load_companies(conn)
    permits = load_permits(conn)
    emails = load_emails(conn)
    doc_links, doc_latest = load_doc_links(INDEX_CSV, conn)
    unmatched_emails = load_unmatched_emails()

    # link_prefix の自動判定
    out_path = Path(args.output)
    if args.link_prefix is None:
        # 出力先が FDE_MANAGED/ 配下なら空、それ以外は 'FDE_MANAGED/'
        try:
            out_path.resolve().relative_to((PROJECT / "output" / "FDE_MANAGED").resolve())
            file_prefix = ""
        except ValueError:
            file_prefix = "FDE_MANAGED/"
    else:
        file_prefix = args.link_prefix
    print(f"link_mode={args.link_mode}, file_prefix={file_prefix!r}")

    print(f"ACTIVE 会社: {len(companies)}")
    print(f"permits(current=1): {len(permits)}")
    print(f"emails: {len(emails)}")
    print(f"doc_links 取得: {len(doc_links)} 会社")
    print(f"未特定取引先(メアド): {len(unmatched_emails)} 件")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "継続取引業者リスト"

    # Row 1: 凡例（全列マージ）
    ws.cell(1, 1).value = LEGEND_TEXT
    ws.cell(1, 1).font = LEGEND_FONT
    ws.cell(1, 1).alignment = Alignment(horizontal="left", vertical="center")
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=N_COLS)
    ws.row_dimensions[1].height = 28

    # Row 2: ヘッダ
    for c, h in enumerate(HEADERS, 1):
        cell = ws.cell(2, c, h)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER
        cell.border = THIN_BORDER

    # 通常会社行
    for i, c in enumerate(companies, start=3):
        cid = c["company_id"]
        permit = permits.get(cid)
        email = emails.get(cid, "")
        is_construction = bool(permit)  # 許可保有 = 建設業
        company_doc_links = doc_links.get(cid, {})
        latest_received = doc_latest.get(cid, "")

        # 受領 doc_id セット（applicable に該当する分）
        received_ids = set(company_doc_links.keys())

        # 必要書類 (F-N) ○/×/—
        required_count = 0
        received_count = 0
        for ci, (display, doc_id, applies_to) in enumerate(DOC_COLS):
            col = COL_DOC_START + ci
            applies = (applies_to == "all_active") or (applies_to == "has_construction_permit" and is_construction)
            cell = ws.cell(i, col)
            cell.alignment = CENTER
            cell.border = THIN_BORDER
            if not applies:
                cell.value = "—"
                cell.fill = GRAY_FILL
                cell.font = GRAY_FONT
                continue
            required_count += 1
            link = company_doc_links.get(doc_id)
            if link:
                received_count += 1
                cell.value = "○"
                cell.fill = GREEN_FILL
                if args.link_mode == "viewer":
                    target = to_link_target_viewer(cid, link.get("page_id"), args.viewer_base)
                else:
                    target = to_link_target_file(link["rel_path"], link["page_from"], file_prefix)
                cell.hyperlink = target
                cell.font = LINK_FONT
            else:
                cell.value = "×"
                cell.fill = RED_FILL

        # 会社名
        ws.cell(i, COL_NAME, c["official_name"]).alignment = LEFT
        # メール
        ws.cell(i, COL_EMAIL, email).alignment = LEFT

        # 許可番号
        if permit:
            ws.cell(i, COL_PERMIT_GENERAL, extract_permit_category(
                permit.get("permit_number"), permit.get("permit_category"))).alignment = CENTER
            ws.cell(i, COL_PERMIT_NUMBER, extract_permit_number_tail(permit.get("permit_number"))).alignment = CENTER
            ws.cell(i, COL_PERMIT_AUTHORITY, parse_authority(permit.get("permit_authority"))).alignment = CENTER

        # 業種マトリクス
        if permit:
            for idx, (abbr, full) in enumerate(TRADE_ABBR):
                col = COL_TRADE_START + idx
                cell = ws.cell(i, col)
                cell.alignment = CENTER
                cell.border = THIN_BORDER
                if full in permit.get("trades", set()):
                    cell.value = "○"

        # 日付
        if permit and permit.get("expiry_date"):
            ws.cell(i, COL_EXPIRY, to_wareki(permit["expiry_date"])).alignment = CENTER
        if latest_received:
            ws.cell(i, COL_RECEIVED, to_wareki(latest_received)).alignment = CENTER

        # データ保存場所（ハイパーリンク）
        cfolder = find_company_folder(cid)
        if cfolder:
            target_disp = f"data/originals/{cfolder.name}/"
            if args.link_mode == "viewer":
                target_link = f"{args.viewer_base}/viewer/{cid}"
            else:
                target_link = f"{file_prefix}10_originals/{cfolder.name}/"
            link_cell = ws.cell(i, COL_FOLDER, target_disp)
            link_cell.hyperlink = target_link
            link_cell.font = LINK_FONT
            link_cell.alignment = LEFT

        # 現在ステータス
        has_any_doc = bool(received_ids)
        status_text, style_key = determine_status(permit, has_any_doc)
        status_cell = ws.cell(i, COL_STATUS, status_text)
        status_cell.alignment = CENTER

        # スタイル
        if style_key == "gray":
            ws.cell(i, COL_NAME).font = GRAY_FONT
            status_cell.font = GRAY_FONT
        elif style_key == "red":
            ws.cell(i, COL_EXPIRY).font = RED_FONT
            status_cell.font = RED_FONT
        elif style_key == "orange":
            ws.cell(i, COL_EXPIRY).font = ORANGE_FONT
            status_cell.font = ORANGE_FONT
        elif style_key == "yellow_bg":
            for c_idx in range(1, N_COLS + 1):
                ws.cell(i, c_idx).fill = YELLOW_FILL

        # 全行に枠線（漏れ補完）
        for col in range(1, N_COLS + 1):
            ws.cell(i, col).border = THIN_BORDER

    # 未特定取引先（メアド placeholder）
    base_row = 3 + len(companies)
    for j, em in enumerate(sorted(unmatched_emails)):
        i = base_row + j
        ws.cell(i, COL_NAME, em).alignment = LEFT
        ws.cell(i, COL_NAME).font = GRAY_FONT
        ws.cell(i, COL_EMAIL, em).alignment = LEFT
        ws.cell(i, COL_EMAIL).font = GRAY_FONT
        ws.cell(i, COL_STATUS, "未特定").alignment = CENTER
        ws.cell(i, COL_STATUS).font = GRAY_FONT
        for col in range(1, N_COLS + 1):
            ws.cell(i, col).border = THIN_BORDER

    total_rows = len(companies) + len(unmatched_emails)
    print(f"\n  会社 {len(companies)} + 未特定 {len(unmatched_emails)} = {total_rows} 行出力")

    # 列幅
    widths = {1: 30, 2: 10, 3: 12, 4: 12, 5: 28}
    for c in range(COL_DOC_START, COL_DOC_END + 1):
        widths[c] = 7   # 必要書類列
    widths[COL_PERMIT_GENERAL] = 4
    widths[COL_PERMIT_GENERAL + 1] = 4
    widths[COL_PERMIT_NUMBER] = 8
    widths[COL_PERMIT_AUTHORITY] = 14
    for c in range(COL_TRADE_START, COL_TRADE_END + 1):
        widths[c] = 4 if len(HEADERS[c - 1]) <= 1 else 5
    widths.update({
        COL_EXPIRY: 12, COL_RENEWAL_CHECK: 12, COL_RECEIVED: 12,
        COL_FOLDER: 40, COL_STATUS: 14, COL_ORDERS: 14,
    })
    for col_idx, w in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = w

    ws.freeze_panes = f"{get_column_letter(COL_DOC_START)}3"
    ws.auto_filter.ref = f"A2:{get_column_letter(N_COLS)}{ws.max_row}"

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)
    print(f"\n✓ {out_path}")
    print(f"  列数: {N_COLS}（必要書類 9 列 + 業種 29 列 + その他）")


if __name__ == "__main__":
    main()
