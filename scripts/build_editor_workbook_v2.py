"""
Phase R-E: 担当者編集ワークブック v2 (9 シート、pre-filled、藤田はその場修正可)。

設計原則 (GPT-5.5 反映 + ユーザー追加要望):
- 当方が全項目を pre-fill、藤田はその場で必要箇所のみ修正
- 機械列 (decision_id/company_id/元値/ハッシュ) は保護
- 編集列はドロップダウン限定 (自由文はコメント欄のみ)
- 藤田が修正できる項目: 業務判定 / 書類状態 / 不要書類 / 会社基本情報 / 許可証情報 / ページ別 doc_type 分類 / コメント
- 藤田が追加できる項目: 新規受領ファイル

シート構成:
  0. workflow_meta            (機械保護)
  1. 突合せ確認              (Phase R-A 結果)
  2. 受領状況編集            (藤田指摘 ベース)
  3. 期限管理                (期限近 + MLIT)
  4. 新規候補                (Phase R-B で追加した社の確認のみ)
  5. 既存対象外化候補        (当方 ACTIVE → 藤田 対象外)
  6. 揃い vs 有効 並列表     (参照のみ)
  7. ページ別分類確認        (Phase R-C ページ再登録の藤田確認、新規)
  8. 会社基本情報修正        (新規)
  9. 新規受領ファイル追加    (新規、藤田から PDF 提供)

Usage:
  python scripts/build_editor_workbook_v2.py [--ts <TS>] [--fujita-ts <FUJITA_TS>] [--business-status-csv <CSV>]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Protection
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
V2_XLSX = PROJECT / "output" / "FDE_MANAGED" / "00_一覧表_v2_20260427.xlsx"
FUJITA_XLSX_DEFAULT = Path(r"C:/Users/owner/Downloads/藤田さん作成　20260428集計中◉継続取引業者リスト.xlsx")

DOC_COLS = ["取引申請書", "建設業許可証", "決算書", "工事経歴書",
             "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]

COLOR_META = "BDD7EE"
COLOR_HEADER = "305496"
COLOR_LOCKED = "F2F2F2"
COLOR_EDIT = "FFF2CC"
COLOR_PREFILL = "E2EFDA"   # 当方 pre-fill 値 (薄緑)
COLOR_HASH = "EDEDED"


def hash_decision(*parts) -> str:
    s = "|".join(str(p) if p is not None else "" for p in parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def make_uuid() -> str:
    return str(uuid.uuid4())


def load_v2() -> tuple[list[dict], dict]:
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
            "permit_authority": ws.cell(r, 7).value,
            "category": ws.cell(r, 8).value,
            "permit_year": ws.cell(r, 9).value,
            "permit_number": ws.cell(r, 10).value,
            "issue_date": ws.cell(r, 11).value,
            "expiry": ws.cell(r, 12).value,
            "trade_kinds": ws.cell(r, 13).value,
            "soroi": ws.cell(r, 22).value,
            "remark": ws.cell(r, 25).value,
        }
        for i, dc in enumerate(DOC_COLS):
            row[dc] = ws.cell(r, 14 + i).value
        rows.append(row)
    return rows, {r["company_id"]: r for r in rows}


def load_fujita(xlsx: Path) -> dict:
    wb = openpyxl.load_workbook(xlsx, data_only=True)
    keizoku = wb["継続取引業者リスト"]
    shusei = wb["修正依頼"]

    keizoku_rows = []
    for r in range(3, keizoku.max_row + 1):
        nm = keizoku.cell(r, 2).value
        if not isinstance(nm, str) or nm in ("会社名", "-"):
            continue
        keizoku_rows.append({
            "fujita_row_id": r,
            "fujita_name": nm,
            "fujita_status": keizoku.cell(r, 44).value,
            "permit_number": keizoku.cell(r, 7).value,
            "permit_authority": keizoku.cell(r, 10).value,
            "expiry": keizoku.cell(r, 40).value,
            "memo": keizoku.cell(r, 48).value,
            "rep_name": keizoku.cell(r, 4).value,
            "contact_person": keizoku.cell(r, 5).value,
            "contact_email": keizoku.cell(r, 6).value,
        })

    shusei_rows = []
    for r in range(2, shusei.max_row + 1):
        nm = shusei.cell(r, 1).value
        if not nm:
            continue
        row = {
            "shusei_row": r,
            "fujita_name": nm,
            "state": shusei.cell(r, 4).value,
            "soroi": shusei.cell(r, 20).value,
            "request": shusei.cell(r, 32).value,
            "fuji_status": shusei.cell(r, 33).value,
        }
        for i, dc in enumerate(DOC_COLS):
            row[dc] = shusei.cell(r, 12 + i).value
        shusei_rows.append(row)

    return {"keizoku": keizoku_rows, "shusei": shusei_rows}


def load_mapping(conn) -> dict[int, dict]:
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
    return out


def load_business_status(csv_path: Path) -> dict[str, dict]:
    out = {}
    with csv_path.open("r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            out[row["company_id"]] = row
    return out


def load_pages_for_company(conn, company_id: str) -> list[dict]:
    """company_id の全ページ (ファイル + ページ番号 + doc_type)"""
    out = []
    for r in conn.execute(
        "SELECT p.file_name, p.page_no, p.doc_type_name, p.confidence "
        "FROM pages p WHERE p.company_id=? "
        "ORDER BY p.file_name, p.page_no",
        (company_id,),
    ):
        out.append({
            "file_name": r[0], "page_no": r[1],
            "doc_type": r[2], "confidence": r[3],
        })
    return out


def load_corrections_csv(csv_path: Path) -> list[dict]:
    out = []
    with csv_path.open("r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            out.append(row)
    return out


def style_header(ws, n_cols: int):
    for c in range(1, n_cols + 1):
        cell = ws.cell(1, c)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor=COLOR_HEADER)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def add_dropdown(ws, col_letter: str, max_row: int, options: list[str], allow_blank=True):
    if max_row < 2:
        return  # データ行がない場合は data validation 適用不要
    formula = '"' + ",".join(options) + '"'
    dv = DataValidation(type="list", formula1=formula, allow_blank=allow_blank, showDropDown=False)
    dv.error = "リストから選択してください"
    dv.errorTitle = "入力エラー"
    ws.add_data_validation(dv)
    dv.add(f"{col_letter}2:{col_letter}{max_row}")


def style_columns(ws, locked: list[int], edit: list[int], prefill: list[int],
                   max_row: int, hash_cols: list[int] = None):
    hash_cols = hash_cols or []
    for c in locked:
        for r in range(2, max_row + 1):
            cell = ws.cell(r, c)
            cell.fill = PatternFill("solid", fgColor=COLOR_HASH if c in hash_cols else COLOR_LOCKED)
            cell.protection = Protection(locked=True)
    for c in prefill:
        for r in range(2, max_row + 1):
            cell = ws.cell(r, c)
            cell.fill = PatternFill("solid", fgColor=COLOR_PREFILL)
            cell.protection = Protection(locked=False)
    for c in edit:
        for r in range(2, max_row + 1):
            cell = ws.cell(r, c)
            cell.fill = PatternFill("solid", fgColor=COLOR_EDIT)
            cell.protection = Protection(locked=False)


def setup_meta(ws, workflow_id, ts, fujita_ts, decision_count):
    rows = [
        ("workflow_id", workflow_id),
        ("発行日時", ts),
        ("対象 fujita_file_ts", fujita_ts),
        ("対象 v2 ファイル", str(V2_XLSX.relative_to(PROJECT))),
        ("expected_decision_count", decision_count),
        ("schema_version", "v2"),
        ("注意",
         "黄色背景 = 編集可能、薄緑 = 当方 pre-fill 値 (修正可能)、灰色 = 機械列 (編集禁止)。"
         "ドロップダウン値以外を入力するとエラーになります。"
         "行の追加・削除はしないでください (新規受領 PDF はシート 9 に追加可)。"),
    ]
    for i, (k, v) in enumerate(rows, 1):
        c = ws.cell(i, 1, k)
        c.font = Font(bold=True)
        c.fill = PatternFill("solid", fgColor=COLOR_META)
        ws.cell(i, 2, v)
    ws.column_dimensions["A"].width = 24
    ws.column_dimensions["B"].width = 100
    ws.protection.sheet = True
    ws.protection.password = "fde_meta_lock"


def build_sheet2b_all_corrections(wb, fujita_data, mapping, v2_by_id, corrections):
    """全修正依頼トラッキングシート (GPT-5.5 v4 review 反映: read-only 内部監査用)

    GPT-5.5 Issue 4 反映:
    - 「処理状況」は手入力廃止、apply 後の audit script (audit_corrections_status.py) で機械生成
    - 本シートは藤田 81 件全件の存在保証 + 反映先案内 (read-only)

    各行に:
    - 藤田原文
    - 自動分類カテゴリ
    - 反映先シート参照 (Sheet 2/7/5/8 etc)
    - 想定処理状況 (apply 後に audit script で更新)
    """
    ws = wb.create_sheet("2b_全修正依頼トラッキング")
    headers = [
        "decision_id",                    # 1: 機械
        "shusei_row",                     # 2: 機械
        "company_id",                     # 3: 機械
        "マスタ名",                        # 4: 機械
        "修正依頼原文",                    # 5: 機械
        "自動分類カテゴリ",                # 6: pre-fill (緑、read-only)
        "推奨反映先シート",                # 7: pre-fill (緑、read-only)
        "想定処理状況 (audit 後更新)",      # 8: 機械 (read-only)
        "備考 (apply 後 audit が更新)",     # 9: 機械 (read-only)
    ]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    # shusei_row → categories の逆引き (1 つの修正依頼が複数カテゴリに分かれる)
    from collections import defaultdict
    row_to_cats = defaultdict(set)
    for c in corrections:
        try:
            sr = int(c["shusei_row"])
            row_to_cats[sr].add(c["category"])
        except (ValueError, KeyError):
            pass

    # カテゴリ → 推奨シート のマッピング
    CAT_TO_SHEET = {
        "PAGE_REREGISTER":     "Sheet 7 (ページ別分類確認)",
        "EMAIL_REIMPORT":      "Sheet 9 (新規受領ファイル) 経由 or 当方再取込",
        "EXEMPTION":           "exemption テーブル (自動登録) or Sheet 5",
        "REJECTED":            "Sheet 2 (受領状況) で「却下」+「要再依頼」",
        "STILL_MISSING":       "Sheet 2 で × 維持、督促リスト",
        "COMPANY_RENAME":      "Sheet 8 (会社情報修正) ※ 会社名は当方マスタ確認後",
        "OTHER":               "本シートで担当者判断",
        "ALREADY_OK":          "対応不要 (既に ○)",
        "EMPTY":               "対応不要",
    }

    rows_to_write = []
    for sh in fujita_data["shusei"]:
        req = sh.get("request") or ""
        if not str(req).strip():
            continue
        nm = sh.get("fujita_name") or ""
        m = fnm_to_map.get(nm)
        cid = m["company_id"] if m else ""
        master_name = v2_by_id.get(cid, {}).get("master_name") or nm
        cats = row_to_cats.get(sh["shusei_row"], set())
        cat_str = " + ".join(sorted(cats)) if cats else "(未分類)"
        sheets_recommend = " / ".join(sorted(set(CAT_TO_SHEET.get(c, "") for c in cats if c in CAT_TO_SHEET)))
        rows_to_write.append({
            "decision_id": make_uuid(),
            "shusei_row": sh["shusei_row"],
            "company_id": cid,
            "name": master_name,
            "request": str(req)[:300],
            "cats": cat_str,
            "sheets": sheets_recommend or "本シートで判断",
        })

    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["decision_id"])
        ws.cell(i, 2, r["shusei_row"])
        ws.cell(i, 3, r["company_id"])
        ws.cell(i, 4, r["name"])
        ws.cell(i, 5, r["request"])
        ws.cell(i, 6, r["cats"])
        ws.cell(i, 7, r["sheets"])
        ws.cell(i, 8, "未反映 (apply 前)")
        ws.cell(i, 9, "")
    last = len(rows_to_write) + 1
    # GPT-5.5 Issue 4: 全列 read-only (機械列扱い)、編集列なし
    style_columns(ws, locked=[1, 2, 3, 4, 5, 8, 9],
                   edit=[], prefill=[6, 7],
                   max_row=last, hash_cols=[1])
    widths = [38, 10, 10, 24, 60, 24, 36, 28, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "E2"
    ws.protection.sheet = True
    ws.protection.password = "fde_view_lock"   # 完全 read-only
    return len(rows_to_write)


def build_sheet1_mapping(wb, mapping, v2_by_id):
    ws = wb.create_sheet("1_突合せ確認")
    headers = ["mapping_hash", "fujita_row_id", "fujita_name", "推定company_id",
                "当方name", "confidence", "推定理由",
                "判定 [同一/別社/新規候補/保留]", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    target = [m for m in mapping.values() if m["confidence"] in ("MEDIUM", "LOW", "UNMATCHED")]
    target.sort(key=lambda x: (x["confidence"], x["fujita_row_id"]))
    for i, m in enumerate(target, 2):
        cid = m["company_id"]
        v2nm = v2_by_id.get(cid, {}).get("master_name", "") if cid else ""
        ws.cell(i, 1, hash_decision("map", m["fujita_row_id"], m["fujita_name"]))
        ws.cell(i, 2, m["fujita_row_id"])
        ws.cell(i, 3, m["fujita_name"])
        ws.cell(i, 4, cid or "")
        ws.cell(i, 5, v2nm)
        ws.cell(i, 6, m["confidence"])
        ws.cell(i, 7, m["match_method"])
    last = len(target) + 1
    style_columns(ws, locked=[1, 2, 3, 4, 5, 6, 7], edit=[8, 9], prefill=[],
                   max_row=last, hash_cols=[1])
    add_dropdown(ws, "H", last, ["同一", "別社", "新規候補", "保留"])
    widths = [18, 12, 30, 14, 30, 12, 22, 28, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "B2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(target)


def build_sheet2_decisions(wb, fujita_data, mapping, v2_by_id, corrections=None):
    """受領状況編集 (修正依頼コメントの category から反映を pre-fill)

    pre-fill ロジック (corrections CSV ベース):
    - PAGE_REREGISTER (P○ 添付/会社概要記載) → 反映 + ○ (受領済の主張)
    - EMAIL_REIMPORT (メール添付) → 反映 + ○
    - EXEMPTION (卸業者等) → 反映 + 対象外
    - REJECTED (拒否) → 却下
    - STILL_MISSING (未提出) → (空、× 維持)
    - その他 (mention のみ、分類不能) → (空、要人間判断)
    """
    ws = wb.create_sheet("2_受領状況編集")
    # corrections (cid, doc_type) → category 逆引き
    cid_doc_to_cat = {}
    if corrections:
        for c in corrections:
            cid = c.get("company_id")
            doc = c.get("doc_type", "")
            if not cid or doc not in DOC_COLS:
                continue
            cat = c.get("category")
            cid_doc_to_cat.setdefault((cid, doc), set()).add(cat)
    headers = ["decision_id", "company_id", "fujita_row_id", "マスタ名",
                "書類カテゴリ", "藤田の値", "当方v2の値", "藤田指摘原文",
                "担当者判断 [反映/保留/却下]", "反映後の値 [○/×/対象外/-]",
                "分類 [受領済/対象外/代替資料で可/要再依頼/確認中]",
                "コメント (自由記入)", "ハッシュ"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    rows_to_write = []
    for sh_row in fujita_data["shusei"]:
        fnm = sh_row["fujita_name"]
        m = fnm_to_map.get(fnm)
        cid = m["company_id"] if m else None
        v2 = v2_by_id.get(cid, {}) if cid else {}
        for dc in DOC_COLS:
            f_val = sh_row.get(dc)
            v_val = v2.get(dc)
            mention = sh_row.get("request") and dc in str(sh_row["request"])
            if f_val == v_val and not mention:
                continue
            # pre-fill ロジック: corrections の category から判定
            # company_id 不明な行は pre-fill しない
            prefill_judge = ""
            prefill_val = ""
            if cid:
                cats = cid_doc_to_cat.get((cid, dc), set())
                v_norm = str(v_val).strip() if v_val else ""
                if "EXEMPTION" in cats:
                    # 卸業者等 → 対象外
                    prefill_judge = "反映"
                    prefill_val = "対象外"
                elif "REJECTED" in cats:
                    # 拒否 → 却下
                    prefill_judge = "却下"
                elif "PAGE_REREGISTER" in cats or "EMAIL_REIMPORT" in cats:
                    # P○ 添付・会社概要記載・メール添付 → 受領済の主張、当方 v2 に反映で ○
                    if v_norm not in ("○", "〇"):
                        prefill_judge = "反映"
                        prefill_val = "○"
                # STILL_MISSING / OTHER / 分類なし → pre-fill しない (人間判断)
            rows_to_write.append({
                "decision_id": make_uuid(),
                "company_id": cid or "",
                "fujita_row_id": sh_row["shusei_row"],
                "name": fnm, "doc": dc,
                "fuji_val": f_val or "", "v2_val": v_val or "",
                "request": sh_row.get("request") or "",
                "prefill_judge": prefill_judge,
                "prefill_val": prefill_val,
            })

    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["decision_id"])
        ws.cell(i, 2, r["company_id"])
        ws.cell(i, 3, r["fujita_row_id"])
        ws.cell(i, 4, r["name"])
        ws.cell(i, 5, r["doc"])
        ws.cell(i, 6, r["fuji_val"])
        ws.cell(i, 7, r["v2_val"])
        ws.cell(i, 8, r["request"])
        # pre-fill (薄緑、編集可)
        ws.cell(i, 9, r["prefill_judge"])
        ws.cell(i, 10, r["prefill_val"])
        ws.cell(i, 13, hash_decision("doc", r["company_id"], r["doc"], r["fuji_val"], r["v2_val"]))
    last = len(rows_to_write) + 1
    # pre-fill 列 (9, 10) は薄緑、修正可能
    style_columns(ws, locked=[1, 2, 3, 4, 5, 6, 7, 8, 13],
                   edit=[11, 12], prefill=[9, 10], max_row=last, hash_cols=[1, 13])
    add_dropdown(ws, "I", last, ["反映", "保留", "却下"])
    add_dropdown(ws, "J", last, ["○", "×", "対象外", "-"])
    add_dropdown(ws, "K", last, ["受領済", "対象外", "代替資料で可", "要再依頼", "確認中"])
    widths = [38, 10, 12, 30, 18, 8, 8, 40, 22, 22, 30, 30, 18]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "E2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows_to_write)


def build_sheet3_expiry(wb, business_status, v2_by_id):
    ws = wb.create_sheet("3_期限管理")
    headers = ["decision_id", "company_id", "マスタ名", "当方期限 (添付許可証)",
                "MLIT期限", "業務判定 (当方算出)",
                "期限ステータス [MLIT更新確認済/藤田に再依頼/期限切れ確定/督促済]",
                "次アクション期日", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    target = [bs for bs in business_status.values()
              if bs["業務判定"] in ("期限切れ", "期限90日以内")]
    for i, bs in enumerate(target, 2):
        ws.cell(i, 1, make_uuid())
        ws.cell(i, 2, bs["company_id"])
        ws.cell(i, 3, bs["master_name"])
        ws.cell(i, 4, bs["expiry"])
        ws.cell(i, 5, "")  # MLIT 期限は別途取得
        ws.cell(i, 6, bs["業務判定"])
    last = len(target) + 1
    style_columns(ws, locked=[1, 2, 3, 4, 5, 6], edit=[7, 8, 9], prefill=[],
                   max_row=last, hash_cols=[1])
    add_dropdown(ws, "G", last,
                 ["MLIT更新確認済", "藤田に再依頼", "期限切れ確定", "督促済"])
    widths = [38, 10, 30, 14, 14, 14, 32, 14, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(target)


def build_sheet4_added(wb, conn):
    """Phase R-B で追加した社の確認のみ (実際の DB INSERT は別フロー)"""
    ws = wb.create_sheet("4_新規追加済確認")
    headers = ["company_id", "official_name", "permit_number", "permit_authority",
                "藤田確認 [OK/修正必要]", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    # Phase R-B で追加されるはずの C0152〜 を取得
    rows = list(conn.execute(
        "SELECT company_id, official_name, permit_number, permit_authority "
        "FROM companies WHERE CAST(SUBSTR(company_id, 2) AS INTEGER) >= 152 "
        "ORDER BY company_id"
    ))
    for i, r in enumerate(rows, 2):
        ws.cell(i, 1, r[0])
        ws.cell(i, 2, r[1])
        ws.cell(i, 3, r[2] or "")
        ws.cell(i, 4, r[3] or "")
    last = len(rows) + 1
    style_columns(ws, locked=[1, 2, 3, 4], edit=[5, 6], prefill=[], max_row=last)
    add_dropdown(ws, "E", last, ["OK", "修正必要"])
    widths = [12, 30, 14, 16, 22, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows)


def build_sheet5_inactive(wb, fujita_data, mapping, v2_by_id):
    ws = wb.create_sheet("5_既存対象外化候補")
    headers = ["decision_id", "company_id", "当方name", "現状態",
                "藤田判定理由 (memo)",
                "処理 [INACTIVE化/対象外フラグ/維持]",
                "理由 [取引終了/廃業/重複/その他]", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    rows_to_write = []
    for k in fujita_data["keizoku"]:
        if k.get("fujita_status") != "対象外":
            continue
        m = fnm_to_map.get(k["fujita_name"])
        if not m or not m["company_id"]:
            continue
        v2 = v2_by_id.get(m["company_id"], {})
        if v2.get("state") != "ACTIVE":
            continue
        rows_to_write.append({
            "decision_id": make_uuid(),
            "company_id": m["company_id"],
            "name": v2.get("master_name") or k["fujita_name"],
            "state": v2.get("state"),
            "memo": k.get("memo") or "",
        })

    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["decision_id"])
        ws.cell(i, 2, r["company_id"])
        ws.cell(i, 3, r["name"])
        ws.cell(i, 4, r["state"])
        ws.cell(i, 5, str(r["memo"])[:200])
    last = len(rows_to_write) + 1
    style_columns(ws, locked=[1, 2, 3, 4, 5], edit=[6, 7, 8], prefill=[], max_row=last)
    add_dropdown(ws, "F", last, ["INACTIVE化", "対象外フラグ", "維持"])
    add_dropdown(ws, "G", last, ["取引終了", "廃業", "重複", "その他"])
    widths = [38, 10, 30, 12, 40, 22, 18, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows_to_write)


def build_sheet6_parallel(wb, business_status, v2_by_id, fujita_data, mapping):
    ws = wb.create_sheet("6_揃いvs有効_並列")
    headers = ["company_id", "name", "当方書類充足度 (n/8)", "実質必要 (n)",
                "exempts", "当方業務判定 (機械算出)", "藤田業務判定", "差分タグ"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    fnm_to_keizoku = {k["fujita_name"]: k for k in fujita_data["keizoku"]}
    cid_to_fuji_status = {}
    for fnm, m in fnm_to_map.items():
        if m["company_id"]:
            cid_to_fuji_status[m["company_id"]] = fnm_to_keizoku.get(fnm, {}).get("fujita_status") or ""

    rows_to_write = []
    for cid in sorted(business_status.keys()):
        bs = business_status[cid]
        v2 = v2_by_id.get(cid, {})
        soroi = v2.get("soroi") or ""
        fuji_status = cid_to_fuji_status.get(cid, "")
        our = bs["業務判定"]
        tag = "✓ 一致" if our == fuji_status else f"差: {our} vs {fuji_status or '(未)'}"
        rows_to_write.append((cid, v2.get("master_name") or v2.get("db_name") or "",
                                soroi, bs["実質必要"], bs["exempts"], our, fuji_status, tag))

    for i, r in enumerate(rows_to_write, 2):
        for c, val in enumerate(r, 1):
            ws.cell(i, c, val)
    last = len(rows_to_write) + 1
    style_columns(ws, locked=list(range(1, 9)), edit=[], prefill=[], max_row=last)
    widths = [10, 30, 14, 12, 24, 18, 18, 24]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_view_lock"
    return len(rows_to_write)


def load_pages_with_id(conn, company_id: str) -> list[dict]:
    """company_id の全ページ (page_id 付き、安定キー)"""
    out = []
    for r in conn.execute(
        "SELECT page_id, file_name, page_no, doc_type_name, confidence "
        "FROM pages WHERE company_id=? "
        "ORDER BY file_name, page_no",
        (company_id,),
    ):
        out.append({
            "page_id": r[0], "file_name": r[1], "page_no": r[2],
            "doc_type": r[3], "confidence": r[4],
        })
    return out


def build_sheet7_pages(wb, conn, corrections):
    """ページ別分類確認 (GPT-5.5 v4 review 反映、安全装置付き)

    GPT-5.5 反映:
    - Issue 1: 「PDF 確認済 [はい/いいえ]」列を追加、必須化 (apply 側で「はい」のみ反映)
    - Issue 3: 単一候補のみ pre-fill、複数候補は file_name 空 + 警告
    """
    ws = wb.create_sheet("7_ページ別分類確認")
    headers = [
        "decision_id",                        # 1: 機械
        "page_id",                            # 2: 機械 (hidden)
        "company_id",                         # 3: 機械
        "マスタ名",                            # 4: 機械
        "対象ファイル (推定)",                 # 5: 機械
        "page_no (藤田指摘 or 当方推定)",      # 6: 機械
        "当方現状 doc_type",                   # 7: 機械
        "藤田希望 doc_type",                   # 8: pre-fill (緑)
        "藤田原文",                            # 9: 機械
        "候補数 (機械)",                       # 10: 機械 (複数なら警告)
        "PDF確認済 [はい/いいえ]",             # 11: 編集 (黄、apply 必須)
        "当方判定 [採用/却下/修正]",           # 12: 編集 (黄)
        "修正後 doc_type (修正選択時)",        # 13: 編集 (黄)
        "確認した実ページ番号 (P不明時)",      # 14: 編集 (黄)
        "コメント",                            # 15: 編集 (黄)
    ]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    # company_id ↔ master_name lookup
    v2_name = {}
    for r in conn.execute("SELECT company_id, official_name FROM companies"):
        v2_name[r[0]] = r[1]

    # PAGE_REREGISTER 全件を行に展開
    rows_to_write = []
    for c in corrections:
        if c.get("category") != "PAGE_REREGISTER":
            continue
        cid = c.get("company_id") or ""
        if not cid:
            continue
        doc = c.get("doc_type", "")
        doc_norm = next((dc for dc in DOC_COLS if dc in doc or doc in dc), None)
        if not doc_norm:
            continue

        page_no = c.get("page_no") or ""
        source_hint = c.get("source_file_hint") or ""
        sub_comment = c.get("sub_comment") or ""

        pages = load_pages_with_id(conn, cid)
        target_page = None
        target_file = ""
        candidate_count = 0
        if page_no:
            try:
                pn = int(page_no)
                # GPT-5.5 Issue 3: source_hint で絞り込み、単一候補のみ採用
                candidates = pages
                if source_hint:
                    hint_filtered = [p for p in pages if source_hint in (p["file_name"] or "")]
                    if hint_filtered:
                        candidates = hint_filtered
                page_match = [p for p in candidates if p["page_no"] == pn]
                candidate_count = len(page_match)
                if candidate_count == 1:
                    # 単一候補のみ pre-fill
                    target_page = page_match[0]
                    target_file = target_page["file_name"]
                # 複数候補は target_page 未設定、apply 側で hard fail
            except (ValueError, TypeError):
                pass
        if not target_page and source_hint:
            hint_match = [p for p in pages if source_hint in (p["file_name"] or "")]
            if len(hint_match) == 1:
                target_file = hint_match[0]["file_name"]
            candidate_count = len(hint_match)

        rows_to_write.append({
            "decision_id": make_uuid(),
            "page_id": target_page["page_id"] if target_page else "",
            "company_id": cid,
            "name": v2_name.get(cid, ""),
            "file_name": target_file,
            "page_no": page_no,
            "current_doc_type": target_page["doc_type"] if target_page else "(未登録)",
            "fuji_doc_type": doc_norm,
            "fuji_comment": sub_comment,
            "candidate_count": candidate_count,
        })

    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["decision_id"])
        ws.cell(i, 2, r["page_id"])
        ws.cell(i, 3, r["company_id"])
        ws.cell(i, 4, r["name"])
        ws.cell(i, 5, r["file_name"])
        ws.cell(i, 6, r["page_no"])
        ws.cell(i, 7, r["current_doc_type"])
        ws.cell(i, 8, r["fuji_doc_type"])
        ws.cell(i, 9, r["fuji_comment"])
        # 候補数列 (Col 10): 複数候補は警告表示
        cc = r.get("candidate_count", 0)
        if cc == 0:
            ws.cell(i, 10, "未登録 (要確認)")
        elif cc == 1:
            ws.cell(i, 10, "1 (確定)")
        else:
            ws.cell(i, 10, f"{cc} (要選別)")
    last = len(rows_to_write) + 1
    # 列順: 1=did, 2=pid(hidden), 3=cid, 4=name, 5=file, 6=page, 7=cur, 8=fuji_doc(prefill),
    #        9=原文, 10=候補数, 11=PDF確認, 12=判定, 13=修正doc, 14=実P, 15=コメ
    style_columns(ws, locked=[1, 2, 3, 4, 5, 6, 7, 9, 10],
                   edit=[11, 12, 13, 14, 15], prefill=[8],
                   max_row=last, hash_cols=[1])
    add_dropdown(ws, "K", last, ["はい", "いいえ"])    # PDF 確認済 (apply 必須)
    add_dropdown(ws, "L", last, ["採用", "却下", "修正"])
    # GPT-5.5 Issue 6: 「その他」は dropdown から削除 (apply 側でも拒否)
    add_dropdown(ws, "M", last, DOC_COLS)
    widths = [38, 10, 10, 22, 30, 10, 16, 16, 40, 14, 16, 18, 16, 14, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.column_dimensions["B"].hidden = True   # page_id
    ws.freeze_panes = "E2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows_to_write)


def build_sheet8_company_info(wb, v2_rows, fujita_data, mapping):
    """会社基本情報修正 (GPT-5.5 Issue 6 反映)
    - DB 反映可能項目のみ編集対象
    - 当方値=空 + 藤田値あり は別ファイル (auto-prefill 候補) に出力、本シートには含めない
    - DB 反映可能項目 = companies テーブルにある列 (許可番号, 行政庁) のみ
    """
    ws = wb.create_sheet("8_会社基本情報修正")
    headers = ["decision_id", "company_id", "項目", "当方値", "藤田値 (参考)",
                "藤田修正値 (上書き、空欄=現状維持)", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    cid_to_fuji = {}
    for k in fujita_data["keizoku"]:
        m = fnm_to_map.get(k["fujita_name"])
        if m and m["company_id"]:
            cid_to_fuji[m["company_id"]] = k

    # DB 反映可能項目のみ
    fields = [("許可番号", "permit_number"),
              ("行政庁", "permit_authority")]

    rows_to_write = []
    auto_prefill_rows = []   # 当方=空 + 藤田=値あり (自動取り込み候補)
    for v2 in v2_rows:
        cid = v2["company_id"]
        f = cid_to_fuji.get(cid, {})
        for field_label, fkey in fields:
            v_val = (v2.get(fkey) or "").strip() if isinstance(v2.get(fkey), str) else (v2.get(fkey) or "")
            f_val = (f.get(fkey) or "").strip() if isinstance(f.get(fkey), str) else (f.get(fkey) or "")
            v_str = str(v_val) if v_val not in (None, "") else ""
            f_str = str(f_val) if f_val not in (None, "") else ""
            if v_str == f_str:
                continue
            if not v_str and f_str:
                # 当方=空 + 藤田=値 → 自動取り込み候補 (藤田に確認させない)
                auto_prefill_rows.append({
                    "company_id": cid, "field": field_label,
                    "v_val": "", "f_val": f_str,
                })
                continue
            if v_str and f_str:
                # 当方=値 + 藤田=値 (異なる) → 藤田に確認 (どちらが正しいか)
                rows_to_write.append({
                    "decision_id": make_uuid(),
                    "company_id": cid, "field": field_label,
                    "v_val": v_str, "f_val": f_str,
                })
            # 当方=値 + 藤田=空 → 当方値維持 (シート対象外)

    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["decision_id"])
        ws.cell(i, 2, r["company_id"])
        ws.cell(i, 3, r["field"])
        ws.cell(i, 4, r["v_val"])
        ws.cell(i, 5, r["f_val"])
    last = len(rows_to_write) + 1
    style_columns(ws, locked=[1, 2, 3, 4, 5], edit=[6, 7], prefill=[],
                   max_row=last, hash_cols=[1])
    widths = [38, 10, 22, 30, 30, 30, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"

    # auto_prefill_rows を CSV 出力 (当方で別途取り込み、藤田に確認させない)
    if auto_prefill_rows:
        out_csv = PROJECT / "output" / "FDE_MANAGED" / f"phase_re_auto_prefill_company_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["company_id", "field", "藤田値", "コメント"])
            for r in auto_prefill_rows:
                w.writerow([r["company_id"], r["field"], r["f_val"], "藤田から自動取り込み候補"])
        print(f"  Sheet8 auto_prefill 候補 (CSV、藤田には見せない): {out_csv.relative_to(PROJECT)} ({len(auto_prefill_rows)} 件)")

    return len(rows_to_write)


def build_hidden_lookup_sheet(wb, v2_by_id):
    """GPT-5.5 Issue 3: hidden sheet に company_id master range を作成
    Sheet 9 の dropdown は範囲参照で 255 文字制限を回避"""
    ws = wb.create_sheet("99_lookup_hidden")
    ws.cell(1, 1, "company_id")
    ws.cell(1, 2, "name")
    cid_list = sorted(v2_by_id.keys())
    for i, cid in enumerate(cid_list, 2):
        ws.cell(i, 1, cid)
        ws.cell(i, 2, v2_by_id[cid].get("master_name") or "")
    ws.sheet_state = "hidden"
    ws.protection.sheet = True
    ws.protection.password = "fde_lookup_lock"
    return len(cid_list)


def add_dropdown_from_range(ws, col_letter: str, max_row: int, source_range: str, allow_blank=True):
    """範囲参照ドロップダウン (255 文字制限なし)"""
    if max_row < 2:
        return
    dv = DataValidation(type="list", formula1=source_range, allow_blank=allow_blank, showDropDown=False)
    dv.error = "リストから選択してください"
    dv.errorTitle = "入力エラー"
    ws.add_data_validation(dv)
    dv.add(f"{col_letter}2:{col_letter}{max_row}")


def build_sheet9_new_files(wb, n_cid):
    """藤田が新規受領 PDF を提供する場 (空、藤田が記入)
    GPT-5.5 Issue 3: dropdown は hidden sheet の範囲参照"""
    ws = wb.create_sheet("9_新規受領ファイル追加")
    headers = ["company_id (ドロップダウン)", "ファイル名 / パス指示",
                "書類カテゴリ (ドロップダウン)", "受領日 (任意)", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers))
    last = 51
    style_columns(ws, locked=[], edit=[1, 2, 3, 4, 5], prefill=[], max_row=last)
    # company_id は hidden sheet 参照 (255 文字制限回避)
    add_dropdown_from_range(ws, "A", last, f"99_lookup_hidden!$A$2:$A${n_cid + 1}")
    add_dropdown(ws, "C", last, DOC_COLS)
    widths = [16, 50, 22, 14, 40]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "A2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument("--fujita-file", type=Path, default=FUJITA_XLSX_DEFAULT)
    ap.add_argument("--fujita-ts", default="20260429_093009")
    ap.add_argument("--business-status-csv", type=Path,
                    default=PROJECT / "data" / "business_status_20260429_phaseRD_initial.csv")
    ap.add_argument("--corrections-csv", type=Path,
                    default=PROJECT / "output" / "FDE_MANAGED" / "10_corrections_classified_20260429_phaseRC1_v2.csv")
    args = ap.parse_args()

    workflow_id = make_uuid()
    print(f"=== Phase R-E: build_editor_workbook v2 ===")
    print(f"  workflow_id: {workflow_id}")
    print(f"  TS: {args.ts}")

    conn = sqlite3.connect(DB_PATH)
    v2_rows, v2_by_id = load_v2()
    fujita_data = load_fujita(args.fujita_file)
    mapping = load_mapping(conn)
    business_status = load_business_status(args.business_status_csv)
    corrections = load_corrections_csv(args.corrections_csv)

    print(f"  v2: {len(v2_rows)}, fujita_keizoku: {len(fujita_data['keizoku'])}, mapping: {len(mapping)}")
    print(f"  business_status: {len(business_status)}, corrections: {len(corrections)}")

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    meta_ws = wb.create_sheet("0_workflow_meta")

    n1 = build_sheet1_mapping(wb, mapping, v2_by_id)
    print(f"  Sheet1 (突合せ確認):       {n1}")
    n2 = build_sheet2_decisions(wb, fujita_data, mapping, v2_by_id, corrections)
    print(f"  Sheet2 (受領状況編集):     {n2}")
    n2b = build_sheet2b_all_corrections(wb, fujita_data, mapping, v2_by_id, corrections)
    print(f"  Sheet2b (全修正依頼):       {n2b}")
    n3 = build_sheet3_expiry(wb, business_status, v2_by_id)
    print(f"  Sheet3 (期限管理):         {n3}")
    n4 = build_sheet4_added(wb, conn)
    print(f"  Sheet4 (新規追加済確認):   {n4}")
    n5 = build_sheet5_inactive(wb, fujita_data, mapping, v2_by_id)
    print(f"  Sheet5 (既存対象外化):     {n5}")
    n6 = build_sheet6_parallel(wb, business_status, v2_by_id, fujita_data, mapping)
    print(f"  Sheet6 (並列表):           {n6}")
    n7 = build_sheet7_pages(wb, conn, corrections)
    print(f"  Sheet7 (ページ別分類):     {n7}")
    n8 = build_sheet8_company_info(wb, v2_rows, fujita_data, mapping)
    print(f"  Sheet8 (会社基本情報):     {n8}")
    # hidden lookup sheet を Sheet 9 の前に作成
    n_cid = build_hidden_lookup_sheet(wb, v2_by_id)
    n9 = build_sheet9_new_files(wb, n_cid)
    print(f"  Sheet9 (新規ファイル追加): 50 空行 (藤田記入用)、cid lookup={n_cid}")

    expected = n1 + n2 + n2b + n3 + n4 + n5 + n7 + n8
    setup_meta(meta_ws, workflow_id, args.ts, args.fujita_ts, expected)

    out = PROJECT / "output" / "FDE_MANAGED" / f"06_担当者編集_v2_{args.ts}_{workflow_id[:8]}.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    conn.close()

    print(f"\n=== Output ===")
    print(f"  {out.relative_to(PROJECT)}")
    print(f"  expected_decision_count: {expected}")


if __name__ == "__main__":
    main()
