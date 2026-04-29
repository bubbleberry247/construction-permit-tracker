"""
Phase R-2: 担当者編集ワークシート (06_担当者編集_<TS>_<workflow_id>.xlsx) 生成。

設計原則 (GPT-5.5 反映):
- workflow_meta シート (機械保護): workflow_id / 発行日時 / 対象v2版 / mapping_version / expected_decision_count
- 機械列 (decision_id, company_id, document_type, 元値, ハッシュ): 保護 + 非表示も検討
- 担当者列: ドロップダウン限定 (data validation)
- 全 decision に UUID v4 を付与
- 「揃い」と「業務有効」を別列で並列保持

シート構成:
  0. workflow_meta (保護)
  1. 突合せ確認        (Phase R-1 LOW/MEDIUM/UNMATCHED)
  2. 受領状況編集     (藤田指摘 81 件 + 当方差分)
  3. 期限管理          (期限近 + 期限切れ + MLIT 更新確認待ち)
  4. 新規候補          (藤田にあって当方ない)
  5. 既存対象外化候補 (当方 ACTIVE → 藤田 対象外)
  6. 揃い vs 有効 並列表

Usage:
  python scripts/build_editor_workbook.py [--ts <TS>]
"""
from __future__ import annotations

import argparse
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

# 色定義
COLOR_META = "BDD7EE"      # 機械保護 (青系)
COLOR_HEADER = "305496"     # ヘッダー (濃紺)
COLOR_LOCKED = "F2F2F2"     # 機械列 (薄灰)
COLOR_EDIT = "FFF2CC"       # 編集列 (薄黄)
COLOR_HASH = "EDEDED"       # ハッシュ列 (極薄灰)


def hash_decision(*parts: str) -> str:
    s = "|".join(str(p) if p is not None else "" for p in parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def make_uuid() -> str:
    return str(uuid.uuid4())


def load_v2_v2() -> tuple[list[dict], openpyxl.Workbook]:
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
            "state": ws.cell(r, 6).value,
            "expiry": ws.cell(r, 12).value,
            "soroi": ws.cell(r, 22).value,
        }
        for i, dc in enumerate(DOC_COLS):
            row[dc] = ws.cell(r, 14 + i).value
        rows.append(row)
    return rows, wb


def load_fujita(xlsx_path: Path) -> dict:
    """藤田さん xlsx 全体を辞書で返す"""
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
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
        })

    # 修正依頼シート: row 2 以降、Col 1=マスタ会社名, Col 32=修正依頼, Col 4=状態, Col 12-19=書類列
    shusei_rows = []
    for r in range(2, shusei.max_row + 1):
        nm = shusei.cell(r, 1).value
        req = shusei.cell(r, 32).value
        if not nm:
            continue
        row = {
            "shusei_row": r,
            "fujita_name": nm,
            "state": shusei.cell(r, 4).value,
            "soroi": shusei.cell(r, 20).value,
            "request": req,
            "fuji_status": shusei.cell(r, 33).value,
        }
        for i, dc in enumerate(DOC_COLS):
            row[dc] = shusei.cell(r, 12 + i).value
        shusei_rows.append(row)

    return {"keizoku": keizoku_rows, "shusei": shusei_rows}


def load_mapping_from_db(fujita_ts: str) -> dict[int, dict]:
    """fujita_row_id -> mapping row"""
    conn = sqlite3.connect(DB_PATH)
    out = {}
    for row in conn.execute(
        "SELECT fujita_row_id, fujita_name, company_id, confidence, match_method "
        "FROM fujita_mapping WHERE fujita_file_ts=?",
        (fujita_ts,),
    ):
        out[row[0]] = {
            "fujita_row_id": row[0], "fujita_name": row[1],
            "company_id": row[2], "confidence": row[3], "match_method": row[4],
        }
    conn.close()
    return out


def setup_workflow_meta(ws, workflow_id: str, ts: str, fujita_ts: str,
                          v2_path: str, decision_count: int):
    """シート 0: workflow_meta"""
    rows = [
        ("workflow_id", workflow_id),
        ("発行日時", ts),
        ("対象 fujita_file_ts", fujita_ts),
        ("対象 v2 ファイル", v2_path),
        ("expected_decision_count", decision_count),
        ("mapping_version", fujita_ts),
        ("注意",
         "このシートは機械処理用です。編集禁止。"
         "他シートの機械列 (灰色) も編集しないでください。"
         "編集してよいのは黄色背景の列のみです。"),
    ]
    for i, (k, v) in enumerate(rows, 1):
        c = ws.cell(i, 1, k)
        c.font = Font(bold=True)
        c.fill = PatternFill("solid", fgColor=COLOR_META)
        ws.cell(i, 2, v)
    ws.column_dimensions["A"].width = 24
    ws.column_dimensions["B"].width = 80
    ws.protection.sheet = True
    ws.protection.password = "fde_meta_lock"


def add_dropdown(ws, col_letter: str, max_row: int, options: list[str], allow_blank=True):
    formula = '"' + ",".join(options) + '"'
    dv = DataValidation(type="list", formula1=formula, allow_blank=allow_blank, showDropDown=False)
    dv.error = "リストから選択してください"
    dv.errorTitle = "入力エラー"
    ws.add_data_validation(dv)
    dv.add(f"{col_letter}2:{col_letter}{max_row}")


def style_header(ws, col_count: int, locked_cols: list[int], edit_cols: list[int]):
    for c in range(1, col_count + 1):
        cell = ws.cell(1, c)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor=COLOR_HEADER)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        if c in locked_cols:
            pass  # ヘッダーはそのまま
        elif c in edit_cols:
            pass


def style_columns(ws, locked_cols: list[int], edit_cols: list[int],
                   max_row: int, hash_cols: list[int] = None):
    hash_cols = hash_cols or []
    for c in locked_cols:
        for r in range(2, max_row + 1):
            cell = ws.cell(r, c)
            cell.fill = PatternFill("solid", fgColor=COLOR_HASH if c in hash_cols else COLOR_LOCKED)
            cell.protection = Protection(locked=True)
    for c in edit_cols:
        for r in range(2, max_row + 1):
            cell = ws.cell(r, c)
            cell.fill = PatternFill("solid", fgColor=COLOR_EDIT)
            cell.protection = Protection(locked=False)


def build_sheet1_mapping(wb, mapping: dict[int, dict], v2_by_id: dict[str, dict]):
    """シート 1: 突合せ確認 (LOW/MEDIUM/UNMATCHED 中心)"""
    ws = wb.create_sheet("1_突合せ確認")
    headers = ["mapping_id", "fujita_row_id", "fujita_name", "推定company_id",
                "当方name", "confidence", "推定理由",
                "判定 [同一/別社/新規候補/保留]", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers), locked_cols=[1, 2, 3, 4, 5, 6, 7], edit_cols=[8, 9])

    # HIGH/REUSED 以外を出す (件数しぼる、HIGH は確定済として別シート参照)
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
    last_row = len(target) + 1
    style_columns(ws, locked_cols=[1, 2, 3, 4, 5, 6, 7], edit_cols=[8, 9],
                  max_row=last_row, hash_cols=[1])
    add_dropdown(ws, "H", last_row, ["同一", "別社", "新規候補", "保留"])

    widths = [18, 12, 30, 14, 30, 12, 22, 28, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "B2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return last_row - 1


def build_sheet2_decisions(wb, fujita_data: dict, mapping: dict[int, dict],
                              v2_by_id: dict[str, dict]):
    """シート 2: 受領状況編集 (藤田指摘 81 件)"""
    ws = wb.create_sheet("2_受領状況編集")
    headers = ["decision_id", "company_id", "fujita_row_id", "マスタ名",
                "書類カテゴリ", "藤田の値", "当方v2の値", "藤田指摘原文",
                "担当者判断 [反映/保留/却下]", "反映後の値 [○/×/対象外/-]",
                "分類 [受領済/対象外/代替資料で可/要再依頼/確認中]",
                "コメント (自由記入)", "ハッシュ"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers), locked_cols=[1, 2, 3, 4, 5, 6, 7, 8, 13],
                  edit_cols=[9, 10, 11, 12])

    # fujita_name -> mapping
    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}

    rows_to_write = []
    for sh_row in fujita_data["shusei"]:
        fnm = sh_row["fujita_name"]
        m = fnm_to_map.get(fnm)
        cid = m["company_id"] if m else None
        v2 = v2_by_id.get(cid, {}) if cid else {}

        # 修正依頼コメントがある社で、書類列ごとに 1 行ずつ展開
        # ただし全 8 書類 × 145 = 1160 行は多いので、藤田 ≠ 当方 の組合せのみ
        for dc in DOC_COLS:
            f_val = sh_row.get(dc)
            v_val = v2.get(dc)
            # diff か、修正依頼にこの書類が含まれていれば対象
            mention = sh_row.get("request") and dc in str(sh_row["request"])
            if f_val == v_val and not mention:
                continue
            rows_to_write.append({
                "decision_id": make_uuid(),
                "company_id": cid or "",
                "fujita_row_id": sh_row["shusei_row"],
                "name": fnm,
                "doc": dc,
                "fuji_val": f_val or "",
                "v2_val": v_val or "",
                "request": sh_row.get("request") or "",
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
        # ハッシュ列
        h = hash_decision("doc", r["company_id"], r["doc"], r["fuji_val"], r["v2_val"])
        ws.cell(i, 13, h)
    last_row = len(rows_to_write) + 1
    style_columns(ws, locked_cols=[1, 2, 3, 4, 5, 6, 7, 8, 13],
                  edit_cols=[9, 10, 11, 12], max_row=last_row, hash_cols=[1, 13])
    add_dropdown(ws, "I", last_row, ["反映", "保留", "却下"])
    add_dropdown(ws, "J", last_row, ["○", "×", "対象外", "-"])
    add_dropdown(ws, "K", last_row, ["受領済", "対象外", "代替資料で可", "要再依頼", "確認中"])

    widths = [38, 10, 12, 30, 18, 8, 8, 40, 22, 22, 30, 30, 18]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "E2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows_to_write)


def build_sheet3_expiry(wb, fujita_data: dict, mapping: dict[int, dict],
                          v2_by_id: dict[str, dict]):
    """シート 3: 期限管理"""
    ws = wb.create_sheet("3_期限管理")
    headers = ["decision_id", "company_id", "マスタ名", "当方期限 (添付許可証)",
                "MLIT期限", "MLIT最終確認日", "藤田判定",
                "期限ステータス [MLIT更新確認済/藤田に再依頼/期限切れ確定/督促済]",
                "次アクション期日", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers), locked_cols=[1, 2, 3, 4, 5, 6, 7], edit_cols=[8, 9, 10])

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    target_status = {"期限90日以内", "期限切れ"}
    rows_to_write = []
    for k_row in fujita_data["keizoku"]:
        if k_row.get("fujita_status") not in target_status:
            continue
        m = fnm_to_map.get(k_row["fujita_name"])
        cid = m["company_id"] if m else ""
        v2 = v2_by_id.get(cid, {}) if cid else {}
        rows_to_write.append({
            "decision_id": make_uuid(),
            "company_id": cid,
            "name": k_row["fujita_name"],
            "v2_expiry": v2.get("expiry") or "",
            "mlit_expiry": "",  # 後で MLIT から取得
            "mlit_check": "",
            "fuji_status": k_row.get("fujita_status"),
        })

    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["decision_id"])
        ws.cell(i, 2, r["company_id"])
        ws.cell(i, 3, r["name"])
        ws.cell(i, 4, str(r["v2_expiry"]))
        ws.cell(i, 5, r["mlit_expiry"])
        ws.cell(i, 6, r["mlit_check"])
        ws.cell(i, 7, r["fuji_status"])
    last_row = len(rows_to_write) + 1
    style_columns(ws, locked_cols=[1, 2, 3, 4, 5, 6, 7], edit_cols=[8, 9, 10],
                  max_row=last_row, hash_cols=[1])
    add_dropdown(ws, "H", last_row,
                 ["MLIT更新確認済", "藤田に再依頼", "期限切れ確定", "督促済"])

    widths = [38, 10, 30, 14, 14, 14, 12, 32, 14, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows_to_write)


def build_sheet4_new_candidates(wb, mapping: dict[int, dict], fujita_data: dict):
    """シート 4: 新規候補 (UNMATCHED)"""
    ws = wb.create_sheet("4_新規候補")
    headers = ["decision_id", "fujita_row_id", "fujita_name", "推定許可番号",
                "藤田判定", "処理 [候補確定/別名疑い/対象外候補]",
                "MLIT確認結果", "提案company_id", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers), locked_cols=[1, 2, 3, 4, 5], edit_cols=[6, 7, 8, 9])

    keizoku_by_row = {k["fujita_row_id"]: k for k in fujita_data["keizoku"]}
    target = [m for m in mapping.values() if m["confidence"] == "UNMATCHED"]
    target.sort(key=lambda x: x["fujita_row_id"])

    for i, m in enumerate(target, 2):
        k = keizoku_by_row.get(m["fujita_row_id"], {})
        ws.cell(i, 1, make_uuid())
        ws.cell(i, 2, m["fujita_row_id"])
        ws.cell(i, 3, m["fujita_name"])
        ws.cell(i, 4, k.get("permit_number") or "")
        ws.cell(i, 5, k.get("fujita_status") or "")
    last_row = len(target) + 1
    style_columns(ws, locked_cols=[1, 2, 3, 4, 5], edit_cols=[6, 7, 8, 9],
                  max_row=last_row, hash_cols=[1])
    add_dropdown(ws, "F", last_row, ["候補確定", "別名疑い", "対象外候補"])

    widths = [38, 12, 30, 14, 12, 24, 24, 14, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(target)


def build_sheet5_inactive_candidates(wb, fujita_data: dict, mapping: dict[int, dict],
                                       v2_by_id: dict[str, dict]):
    """シート 5: 既存対象外化候補 (当方 ACTIVE → 藤田 対象外)"""
    ws = wb.create_sheet("5_既存対象外化候補")
    headers = ["decision_id", "company_id", "当方name", "現状態",
                "藤田判定理由 (memo)",
                "処理 [INACTIVE化/対象外フラグ/維持]",
                "理由 [取引終了/廃業/重複/その他]", "コメント"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers), locked_cols=[1, 2, 3, 4, 5], edit_cols=[6, 7, 8])

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    rows_to_write = []
    for k in fujita_data["keizoku"]:
        if k.get("fujita_status") != "対象外":
            continue
        m = fnm_to_map.get(k["fujita_name"])
        if not m or not m["company_id"]:
            continue  # 当方に存在しない (シート 4 で扱う)
        v2 = v2_by_id.get(m["company_id"], {})
        if v2.get("state") != "ACTIVE":
            continue  # 既に INACTIVE なら対象外
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
    last_row = len(rows_to_write) + 1
    style_columns(ws, locked_cols=[1, 2, 3, 4, 5], edit_cols=[6, 7, 8],
                  max_row=last_row, hash_cols=[1])
    add_dropdown(ws, "F", last_row, ["INACTIVE化", "対象外フラグ", "維持"])
    add_dropdown(ws, "G", last_row, ["取引終了", "廃業", "重複", "その他"])

    widths = [38, 10, 30, 12, 40, 22, 18, 30]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_edit_lock"
    return len(rows_to_write)


def build_sheet6_parallel(wb, fujita_data: dict, mapping: dict[int, dict],
                            v2_by_id: dict[str, dict]):
    """シート 6: 揃い vs 業務有効 並列表 (参照のみ、編集なし)"""
    ws = wb.create_sheet("6_揃いvs有効_並列")
    headers = ["company_id", "name", "当方書類充足度 (n/8)", "当方期限OK",
                "MLIT期限OK", "藤田業務有効判定", "差分タグ"]
    for c, h in enumerate(headers, 1):
        ws.cell(1, c, h)
    style_header(ws, len(headers), locked_cols=list(range(1, 8)), edit_cols=[])

    fnm_to_map = {m["fujita_name"]: m for m in mapping.values()}
    fnm_to_keizoku = {k["fujita_name"]: k for k in fujita_data["keizoku"]}
    rows_to_write = []
    for cid, v2 in v2_by_id.items():
        # この company の藤田判定を取得 (mapping 経由)
        fuji_status = ""
        for fnm, m in fnm_to_map.items():
            if m["company_id"] == cid:
                fuji_status = fnm_to_keizoku.get(fnm, {}).get("fujita_status") or ""
                break
        soroi = v2.get("soroi") or ""
        # 差分タグ計算
        n_soroi = 0
        try:
            n_soroi = int(str(soroi).split("/")[0])
        except (ValueError, IndexError):
            pass
        tag = ""
        if n_soroi == 8 and fuji_status == "有効":
            tag = "✓ 一致"
        elif n_soroi == 0 and fuji_status == "未提出":
            tag = "✓ 一致"
        elif n_soroi == 0 and fuji_status == "対象外":
            tag = "対象外化候補"
        elif fuji_status in ("期限切れ", "期限90日以内"):
            tag = f"期限注意 ({fuji_status})"
        elif n_soroi < 8 and fuji_status == "有効":
            tag = "藤田: 緩い有効判定 (許可有効+補完情報)"
        elif n_soroi == 8 and not fuji_status:
            tag = "藤田判定漏れ"
        elif fuji_status == "書類不備":
            tag = "両者: 書類不備"
        rows_to_write.append({
            "cid": cid,
            "name": v2.get("master_name") or v2.get("db_name") or "",
            "soroi": soroi,
            "v2_expiry_ok": "Y" if v2.get("expiry") else "?",
            "mlit_ok": "?",
            "fuji_status": fuji_status,
            "tag": tag,
        })
    rows_to_write.sort(key=lambda x: x["cid"])
    for i, r in enumerate(rows_to_write, 2):
        ws.cell(i, 1, r["cid"])
        ws.cell(i, 2, r["name"])
        ws.cell(i, 3, r["soroi"])
        ws.cell(i, 4, r["v2_expiry_ok"])
        ws.cell(i, 5, r["mlit_ok"])
        ws.cell(i, 6, r["fuji_status"])
        ws.cell(i, 7, r["tag"])
    last_row = len(rows_to_write) + 1
    style_columns(ws, locked_cols=list(range(1, 8)), edit_cols=[], max_row=last_row)

    widths = [10, 30, 18, 12, 12, 18, 32]
    for c, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.freeze_panes = "C2"
    ws.protection.sheet = True
    ws.protection.password = "fde_view_lock"
    return len(rows_to_write)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    ap.add_argument("--fujita-file", type=Path, default=FUJITA_XLSX_DEFAULT)
    ap.add_argument("--fujita-ts", default="20260429_093009",
                    help="fujita_mapping テーブルの fujita_file_ts (Phase R-1 で投入したもの)")
    args = ap.parse_args()

    workflow_id = make_uuid()
    ts = args.ts
    print(f"workflow_id = {workflow_id}")
    print(f"TS          = {ts}")

    print("Loading v2...")
    v2_rows, _ = load_v2_v2()
    v2_by_id = {r["company_id"]: r for r in v2_rows}
    print(f"  -> {len(v2_rows)} 社")

    print("Loading 藤田 file...")
    fujita_data = load_fujita(args.fujita_file)
    print(f"  -> 継続取引業者リスト: {len(fujita_data['keizoku'])} 社")
    print(f"  -> 修正依頼:           {len(fujita_data['shusei'])} 行")

    print(f"Loading mapping (fujita_ts={args.fujita_ts})...")
    mapping = load_mapping_from_db(args.fujita_ts)
    print(f"  -> {len(mapping)} 件")

    # ワークブック構築
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    meta_ws = wb.create_sheet("0_workflow_meta")

    n_sh1 = build_sheet1_mapping(wb, mapping, v2_by_id)
    print(f"  Sheet1 (突合せ確認):   {n_sh1} 行")
    n_sh2 = build_sheet2_decisions(wb, fujita_data, mapping, v2_by_id)
    print(f"  Sheet2 (受領状況編集): {n_sh2} 行")
    n_sh3 = build_sheet3_expiry(wb, fujita_data, mapping, v2_by_id)
    print(f"  Sheet3 (期限管理):     {n_sh3} 行")
    n_sh4 = build_sheet4_new_candidates(wb, mapping, fujita_data)
    print(f"  Sheet4 (新規候補):     {n_sh4} 行")
    n_sh5 = build_sheet5_inactive_candidates(wb, fujita_data, mapping, v2_by_id)
    print(f"  Sheet5 (対象外化候補): {n_sh5} 行")
    n_sh6 = build_sheet6_parallel(wb, fujita_data, mapping, v2_by_id)
    print(f"  Sheet6 (並列表):       {n_sh6} 行")

    expected_decisions = n_sh1 + n_sh2 + n_sh3 + n_sh4 + n_sh5
    setup_workflow_meta(meta_ws, workflow_id, ts, args.fujita_ts,
                          str(V2_XLSX.relative_to(PROJECT)), expected_decisions)

    out_path = PROJECT / "output" / "FDE_MANAGED" / f"06_担当者編集_{ts}_{workflow_id[:8]}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)
    print()
    print("=== Output ===")
    print(f"  {out_path.relative_to(PROJECT)}")
    print(f"  expected_decision_count = {expected_decisions}")
    print(f"  workflow_id             = {workflow_id}")


if __name__ == "__main__":
    main()
