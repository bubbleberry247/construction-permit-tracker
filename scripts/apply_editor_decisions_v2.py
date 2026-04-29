"""
Phase R-F: 編集ワークブック v2 (9 シート版) の反映スクリプト (GPT-5.5 レビュー反映後)。

GPT-5.5 REQUEST_CHANGES 5 件の反映:
  Issue 2: DB 更新と v3 xlsx 生成を atomic に
           → v3 を tmp 生成 → DB transaction (decisions_log INSERT 込み) → COMMIT → atomic rename
  Issue 3: source_row_hash + edited_value_hash で完全検証
           → 同 workflow_id 再投入時に「内容一致なら no-op、差分あれば hard fail」
  Issue 4: 無回答 = 現状維持 (未確認) を厳格化
           → 空欄は明示的にスキップ、log にも記録しない
  Issue 5: pages 分類変更履歴を page_doc_type_history に保存
           → pages UPDATE 前に旧 doc_type を取得 → history INSERT → UPDATE

新 decision_kind:
  doc_status                       - シート 2 (受領状況)
  expiry_status                    - シート 3 (期限管理)
  inactivate                       - シート 5 (対象外化)
  page_classification_confirm      - シート 7 「正」確認
  page_classification_modify       - シート 7 doc_type 修正
  company_info_update              - シート 8 (会社情報修正)
  file_upload_request              - シート 9 (新規ファイル受付、CSV)

無回答 (空欄) は全シートで「現状維持」扱い、apply 対象外、log 記録なし。

Usage:
  python scripts/apply_editor_decisions_v2.py <06_担当者編集_v2_*.xlsx>
  python scripts/apply_editor_decisions_v2.py <wb> --execute
  python scripts/apply_editor_decisions_v2.py --rollback <TS>
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from shutil import copy2

import openpyxl

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
V2_XLSX = PROJECT / "output" / "FDE_MANAGED" / "00_一覧表_v2_20260427.xlsx"
SNAP_DIR = PROJECT / "data" / "snapshots"

DOC_COLS = ["取引申請書", "建設業許可証", "決算書", "工事経歴書",
             "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]


def stable_hash(*parts) -> str:
    s = "|".join(str(p) if p is not None else "" for p in parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def source_row_hash(d: dict) -> str:
    """機械列 (担当者が編集してはいけないもの) のハッシュ"""
    return stable_hash(
        d.get("decision_id"), d.get("company_id"),
        d.get("書類カテゴリ") or d.get("document_type") or d.get("項目"),
        d.get("当方v2の値") or d.get("当方値") or d.get("当方判定 doc_type") or d.get("現状態"),
    )


def edited_value_hash(decision_value: str, new_value: str, comment: str) -> str:
    return stable_hash(decision_value, new_value, comment)


def load_meta(wb) -> dict:
    ws = wb["0_workflow_meta"]
    meta = {}
    for r in range(1, ws.max_row + 1):
        k = ws.cell(r, 1).value
        v = ws.cell(r, 2).value
        if k:
            meta[k] = v
    return meta


def read_sheet(wb, name: str) -> list[dict]:
    if name not in wb.sheetnames:
        return []
    ws = wb[name]
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    rows = []
    for r in range(2, ws.max_row + 1):
        if all(ws.cell(r, c).value is None for c in range(1, ws.max_column + 1)):
            continue
        rows.append({h: ws.cell(r, c).value for c, h in enumerate(headers, 1) if h})
    return rows


def precheck(meta: dict, all_sheets: dict, conn) -> tuple[list[str], str]:
    """returns (fails, reapply_mode)
    reapply_mode: 'fresh' / 'idempotent_noop' / 'conflict'
    """
    fails = []
    wf = meta.get("workflow_id")
    if not wf:
        fails.append("workflow_id not found in meta")
        return fails, "fresh"
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions_log WHERE workflow_id=?", (wf,)
    ).fetchone()[0]
    if n > 0:
        # 既存 hash との一致を検証
        existing = {row[0]: (row[1], row[2]) for row in conn.execute(
            "SELECT decision_id, source_row_hash, edited_value_hash "
            "FROM decisions_log WHERE workflow_id=?", (wf,)
        )}
        # 全シートの decision_id を集めて hash 突合せ
        all_decision_ids = set()
        diffs = []
        for sheet_name, rows in all_sheets.items():
            for d in rows:
                did = d.get("decision_id")
                if not did:
                    continue
                all_decision_ids.add(did)
                if did in existing:
                    src_hash = source_row_hash(d)
                    if src_hash != existing[did][0]:
                        diffs.append(f"  source_row_hash mismatch for decision_id={did[:8]}... in {sheet_name}")
        if diffs:
            fails.append(f"workflow_id={wf} already applied AND content differs ({len(diffs)} mismatches)")
            for d in diffs[:5]:
                fails.append(d)
            return fails, "conflict"
        # 内容完全一致 → idempotent
        return [], "idempotent_noop"
    return [], "fresh"


def lookup_page_id(conn, company_id: str, file_name: str, page_no) -> int | None:
    """安定 page_id を検索 (file_name + company_id + page_no で同定)"""
    if not (company_id and file_name and page_no is not None):
        return None
    try:
        page_no_int = int(page_no)
    except (ValueError, TypeError):
        return None
    row = conn.execute(
        "SELECT page_id FROM pages WHERE company_id=? AND file_name=? AND page_no=?",
        (company_id, file_name, page_no_int),
    ).fetchone()
    return row[0] if row else None


def apply_v2(wb, conn, execute: bool, ts: str, mode: str) -> dict:
    """
    mode: 'fresh' / 'idempotent_noop'
    無回答 (担当者列が空) はすべてスキップ、log に記録しない。
    機械列は読み取り専用、改変検出は precheck で実施済み。
    """
    meta = load_meta(wb)
    wf_id = meta.get("workflow_id")
    operator = "phase_rf_v2_apply"

    sheets_to_read = ["1_突合せ確認", "2_受領状況編集", "3_期限管理",
                       "4_新規追加済確認", "5_既存対象外化候補",
                       "7_ページ別分類確認", "8_会社基本情報修正",
                       "9_新規受領ファイル追加"]
    sheets = {n: read_sheet(wb, n) for n in sheets_to_read}

    summary = {
        "doc_status_apply": 0, "doc_status_hold": 0, "doc_status_reject": 0,
        "expiry_status": 0, "inactivate": 0,
        "page_classification_modify": 0, "page_classification_confirm": 0,
        "page_classification_reject": 0,
        "company_info_update": 0,
        "file_upload_request": 0, "skipped_no_decision": 0,
    }
    log_rows = []
    v2_updates = []
    file_upload_requests = []
    page_history_rows = []   # Issue 5

    if mode == "idempotent_noop":
        print("  [INFO] idempotent re-apply: 内容完全一致のため、本反映はスキップ (log 重複なし)")
        return {"summary": summary, "log_rows": log_rows,
                "v2_updates": v2_updates, "file_upload_requests": file_upload_requests,
                "page_history_rows": page_history_rows, "wf_id": wf_id, "mode": mode}

    # --- シート 2: 受領状況反映 ---
    for d in sheets["2_受領状況編集"]:
        judge = (d.get("担当者判断 [反映/保留/却下]") or "").strip()
        if not judge:
            summary["skipped_no_decision"] += 1
            continue   # Issue 4: 無回答はスキップ
        if judge == "保留":
            summary["doc_status_hold"] += 1
            continue
        if judge == "却下":
            summary["doc_status_reject"] += 1
            continue
        if judge != "反映":
            continue
        new_val = (d.get("反映後の値 [○/×/対象外/-]") or "").strip()
        if not new_val:
            summary["skipped_no_decision"] += 1
            continue   # 反映選択だが値未指定 = スキップ
        v2_updates.append({
            "company_id": d.get("company_id"),
            "doc_col": d.get("書類カテゴリ"),
            "new_value": new_val,
        })
        cmt = d.get("コメント (自由記入)") or ""
        log_rows.append({
            "decision_id": d.get("decision_id"), "workflow_id": wf_id,
            "company_id": d.get("company_id"),
            "document_type": d.get("書類カテゴリ"),
            "decision_kind": "doc_status",
            "old_value": d.get("当方v2の値"), "new_value": new_val,
            "decision_value": "反映", "operator": operator,
            "fujita_comment": d.get("藤田指摘原文"),
            "operator_comment": cmt,
            "source_workbook": wf_id,
            "source_row_hash": source_row_hash(d),
            "edited_value_hash": edited_value_hash("反映", new_val, cmt),
        })
        summary["doc_status_apply"] += 1

    # --- シート 3: 期限ステータス ---
    for d in sheets["3_期限管理"]:
        st = (d.get("期限ステータス [MLIT更新確認済/藤田に再依頼/期限切れ確定/督促済]") or "").strip()
        if not st:
            continue   # Issue 4
        cmt = d.get("コメント") or ""
        log_rows.append({
            "decision_id": d.get("decision_id"), "workflow_id": wf_id,
            "company_id": d.get("company_id"), "document_type": None,
            "decision_kind": "expiry_status",
            "old_value": d.get("業務判定 (当方算出)"), "new_value": st,
            "decision_value": st, "operator": operator,
            "fujita_comment": None, "operator_comment": cmt,
            "source_workbook": wf_id,
            "source_row_hash": source_row_hash(d),
            "edited_value_hash": edited_value_hash(st, st, cmt),
        })
        if execute:
            conn.execute(
                "INSERT INTO permit_expiry_status (company_id, expiry_status, "
                "next_action, next_action_due, workflow_id, notes) VALUES (?, ?, ?, ?, ?, ?)",
                (d.get("company_id"), st, st, d.get("次アクション期日"),
                 wf_id, cmt),
            )
        summary["expiry_status"] += 1

    # --- シート 5: INACTIVE 化 ---
    for d in sheets["5_既存対象外化候補"]:
        proc = (d.get("処理 [INACTIVE化/対象外フラグ/維持]") or "").strip()
        if proc not in ("INACTIVE化", "対象外フラグ"):
            continue   # 「維持」「空欄」はスキップ
        cmt = d.get("コメント") or ""
        log_rows.append({
            "decision_id": d.get("decision_id"), "workflow_id": wf_id,
            "company_id": d.get("company_id"), "document_type": None,
            "decision_kind": "inactivate",
            "old_value": d.get("現状態"), "new_value": proc,
            "decision_value": proc, "operator": operator,
            "fujita_comment": d.get("藤田判定理由 (memo)"),
            "operator_comment": cmt, "source_workbook": wf_id,
            "source_row_hash": source_row_hash(d),
            "edited_value_hash": edited_value_hash(proc, proc, cmt),
        })
        if execute:
            new_state = "INACTIVE" if proc == "INACTIVE化" else "MANAGED_OUT"
            conn.execute(
                "UPDATE companies SET status=?, updated_at=datetime('now','localtime') "
                "WHERE company_id=?",
                (new_state, d.get("company_id")),
            )
        summary["inactivate"] += 1

    # --- シート 7: ページ別分類確認 (GPT-5.5 v4 review 反映) ---
    # Issue 1: PDF 確認済 [はい/いいえ] 必須
    # Issue 2: page_id 空 OR PDF 確認済 != "はい" → hard fail (apply 対象外)
    # Issue 3: 単一候補のみ pre-fill 済、複数候補は file_name 空 → 上記でも apply 対象外
    # Issue 6: doc_type が DOC_COLS 外 (「その他」等) → apply で拒否
    summary["page_classification_blocked_no_pdf_check"] = 0
    summary["page_classification_blocked_no_page_id"] = 0
    summary["page_classification_blocked_invalid_doc"] = 0
    for d in sheets["7_ページ別分類確認"]:
        verdict = (d.get("当方判定 [採用/却下/修正]") or "").strip()
        if not verdict:
            summary["skipped_no_decision"] += 1
            continue
        if verdict == "却下":
            summary["page_classification_reject"] += 1
            # 却下も log には記録 (監査)
            log_rows.append({
                "decision_id": d.get("decision_id"), "workflow_id": wf_id,
                "company_id": d.get("company_id"),
                "document_type": d.get("当方現状 doc_type") or "",
                "decision_kind": "page_classification_reject",
                "old_value": d.get("当方現状 doc_type") or "",
                "new_value": d.get("当方現状 doc_type") or "",
                "decision_value": "却下",
                "operator": operator, "fujita_comment": d.get("藤田原文"),
                "operator_comment": d.get("コメント") or "", "source_workbook": wf_id,
                "source_row_hash": source_row_hash(d),
                "edited_value_hash": edited_value_hash("却下", "", d.get("コメント") or ""),
            })
            continue

        # GPT-5.5 Issue 1: PDF 確認済フラグ必須
        pdf_checked = (d.get("PDF確認済 [はい/いいえ]") or "").strip()
        if pdf_checked != "はい":
            summary["page_classification_blocked_no_pdf_check"] += 1
            print(f"  [BLOCK] decision_id={str(d.get('decision_id',''))[:8]}... PDF未確認のため apply 対象外 (cid={d.get('company_id')})")
            continue

        cid = d.get("company_id")
        file_name = d.get("対象ファイル (推定)")
        page_no_field = d.get("page_no (藤田指摘 or 当方推定)")
        confirmed_pn = d.get("確認した実ページ番号 (P不明時)")
        cmt = d.get("コメント") or ""
        old_doc = d.get("当方現状 doc_type") or ""
        fuji_doc = d.get("藤田希望 doc_type") or ""
        modified_doc = (d.get("修正後 doc_type (修正選択時)") or "").strip()

        if verdict == "採用":
            new_doc = fuji_doc
        elif verdict == "修正":
            if not modified_doc:
                summary["skipped_no_decision"] += 1
                continue
            new_doc = modified_doc
        else:
            continue

        if not new_doc:
            summary["skipped_no_decision"] += 1
            continue

        # GPT-5.5 Issue 6: DOC_COLS 以外を拒否
        if new_doc not in DOC_COLS:
            summary["page_classification_blocked_invalid_doc"] += 1
            print(f"  [BLOCK] decision_id={str(d.get('decision_id',''))[:8]}... new_doc='{new_doc}' は DOC_COLS 外 (cid={cid})")
            continue

        effective_page_no = confirmed_pn or page_no_field
        page_id_from_sheet = d.get("page_id")
        page_id = None
        if page_id_from_sheet not in (None, ""):
            try:
                page_id = int(page_id_from_sheet)
            except (ValueError, TypeError):
                pass
        if page_id is None:
            page_id = lookup_page_id(conn, cid, file_name, effective_page_no)

        # GPT-5.5 Issue 2: page_id 空 → hard fail
        if page_id is None:
            summary["page_classification_blocked_no_page_id"] += 1
            print(f"  [BLOCK] decision_id={str(d.get('decision_id',''))[:8]}... page_id 解決不能 (cid={cid}, file={file_name}, page={effective_page_no})")
            continue

        log_rows.append({
            "decision_id": d.get("decision_id"), "workflow_id": wf_id,
            "company_id": cid,
            "document_type": new_doc,
            "decision_kind": "page_classification_modify" if old_doc != new_doc else "page_classification_confirm",
            "old_value": old_doc, "new_value": new_doc,
            "decision_value": verdict,
            "operator": operator, "fujita_comment": d.get("藤田原文"),
            "operator_comment": cmt, "source_workbook": wf_id,
            "source_row_hash": source_row_hash(d),
            "edited_value_hash": edited_value_hash(verdict, new_doc, cmt),
        })
        if old_doc != new_doc:
            page_history_rows.append({
                "page_id": page_id,
                "company_id": cid, "file_name": file_name, "page_no": effective_page_no,
                "old_doc_type_name": old_doc, "new_doc_type_name": new_doc,
                "reason": cmt or f"藤田希望: {d.get('藤田原文', '')}",
                "workflow_id": wf_id,
                "decision_id": d.get("decision_id"), "confirmed_by": operator,
            })
            summary["page_classification_modify"] += 1
        else:
            summary["page_classification_confirm"] += 1

    # --- シート 8: 会社基本情報修正 ---
    company_field_map = {
        "許可番号": ("companies", "permit_number"),
        "行政庁": ("companies", "permit_authority"),
    }
    for d in sheets["8_会社基本情報修正"]:
        new_v = (d.get("藤田修正値 (上書き)") or "").strip()
        if not new_v:
            continue   # Issue 4
        field = d.get("項目")
        cmt = d.get("コメント") or ""
        log_rows.append({
            "decision_id": d.get("decision_id"), "workflow_id": wf_id,
            "company_id": d.get("company_id"), "document_type": None,
            "decision_kind": "company_info_update",
            "old_value": d.get("当方値"), "new_value": new_v,
            "decision_value": field, "operator": operator,
            "fujita_comment": d.get("藤田値 (参考)"),
            "operator_comment": cmt, "source_workbook": wf_id,
            "source_row_hash": source_row_hash(d),
            "edited_value_hash": edited_value_hash(field or "", new_v, cmt),
        })
        if execute and field in company_field_map:
            tbl, col = company_field_map[field]
            conn.execute(
                f"UPDATE {tbl} SET {col}=?, updated_at=datetime('now','localtime') "
                f"WHERE company_id=?",
                (new_v, d.get("company_id")),
            )
        summary["company_info_update"] += 1

    # --- シート 9: 新規受領ファイル追加 ---
    for d in sheets["9_新規受領ファイル追加"]:
        cid = (d.get("company_id (ドロップダウン)") or "").strip()
        path_or_name = (d.get("ファイル名 / パス指示") or "").strip()
        if not cid or not path_or_name:
            continue
        file_upload_requests.append({
            "company_id": cid,
            "file_name_or_path": path_or_name,
            "doc_category": d.get("書類カテゴリ (ドロップダウン)") or "",
            "received_date": d.get("受領日 (任意)") or "",
            "comment": d.get("コメント") or "",
            "workflow_id": wf_id,
        })
        summary["file_upload_request"] += 1

    # --- DB INSERT (transaction 内、execute 時のみ) ---
    if execute:
        for lr in log_rows:
            conn.execute(
                """INSERT INTO decisions_log
                (decision_id, workflow_id, company_id, document_type, decision_kind,
                 old_value, new_value, decision_value, operator, fujita_comment,
                 operator_comment, source_workbook, source_row_hash, edited_value_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (lr["decision_id"], lr["workflow_id"], lr["company_id"],
                 lr["document_type"], lr["decision_kind"], lr["old_value"],
                 lr["new_value"], lr["decision_value"], lr["operator"],
                 lr["fujita_comment"], lr["operator_comment"], lr["source_workbook"],
                 lr["source_row_hash"], lr["edited_value_hash"]),
            )
        # Issue 5: page_history INSERT → 然る後 pages UPDATE
        for ph in page_history_rows:
            conn.execute(
                """INSERT INTO page_doc_type_history
                (page_id, company_id, file_name, page_no, old_doc_type_name,
                 new_doc_type_name, reason, workflow_id, decision_id, confirmed_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ph["page_id"], ph["company_id"], ph["file_name"], ph["page_no"],
                 ph["old_doc_type_name"], ph["new_doc_type_name"], ph["reason"],
                 ph["workflow_id"], ph["decision_id"], ph["confirmed_by"]),
            )
            if ph["page_id"]:
                conn.execute(
                    "UPDATE pages SET doc_type_name=? WHERE page_id=?",
                    (ph["new_doc_type_name"], ph["page_id"]),
                )

    return {"summary": summary, "log_rows": log_rows,
            "v2_updates": v2_updates, "file_upload_requests": file_upload_requests,
            "page_history_rows": page_history_rows, "wf_id": wf_id, "mode": mode}


def generate_v3_to_tmp(v2_updates: list[dict], ts: str, wf_id: str) -> tuple[Path, Path]:
    """v3 を .tmp として生成 (DB transaction 前に実行、失敗を先取り検出)。
    返値: (final_path, tmp_path)"""
    wb = openpyxl.load_workbook(V2_XLSX)
    ws = wb["マスタ145社受領状況"]
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    cid_col = headers.index("会社ID") + 1
    doc_col_idx = {dc: headers.index(dc) + 1 for dc in DOC_COLS if dc in headers}
    cid_to_row = {ws.cell(r, cid_col).value: r
                   for r in range(2, ws.max_row + 1) if ws.cell(r, cid_col).value}

    n_applied = 0
    for u in v2_updates:
        r = cid_to_row.get(u["company_id"])
        c = doc_col_idx.get(u["doc_col"])
        if r and c:
            ws.cell(r, c).value = u["new_value"]
            n_applied += 1

    final = PROJECT / "output" / "FDE_MANAGED" / f"00_一覧表_v3_{ts}_{wf_id[:8]}.xlsx"
    tmp = final.with_suffix(".xlsx.tmp")
    final.parent.mkdir(parents=True, exist_ok=True)
    wb.save(tmp)
    print(f"  v3 tmp 生成: {tmp.relative_to(PROJECT)}  反映 {n_applied}/{len(v2_updates)} セル")
    return final, tmp


def write_dryrun_csv(result: dict, ts: str) -> Path:
    out = PROJECT / "output" / "FDE_MANAGED" / f"phase_rf_dryrun_{ts}.csv"
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["decision_id", "workflow_id", "company_id", "document_type",
                    "decision_kind", "old_value", "new_value", "decision_value",
                    "source_row_hash", "edited_value_hash",
                    "fujita_comment", "operator_comment"])
        for lr in result["log_rows"]:
            w.writerow([lr["decision_id"], lr["workflow_id"], lr["company_id"],
                        lr["document_type"], lr["decision_kind"], lr["old_value"],
                        lr["new_value"], lr["decision_value"],
                        lr.get("source_row_hash", "")[:16],
                        lr.get("edited_value_hash", "")[:16],
                        str(lr["fujita_comment"] or "")[:200],
                        str(lr["operator_comment"] or "")[:200]])
    print(f"  dry-run CSV: {out.relative_to(PROJECT)}")

    if result["file_upload_requests"]:
        upload_csv = PROJECT / "output" / "FDE_MANAGED" / f"phase_rf_file_upload_requests_{ts}.csv"
        with upload_csv.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["company_id", "file_name_or_path", "doc_category",
                        "received_date", "comment", "workflow_id"])
            for r in result["file_upload_requests"]:
                w.writerow([r["company_id"], r["file_name_or_path"],
                            r["doc_category"], r["received_date"], r["comment"],
                            r["workflow_id"]])
        print(f"  file_upload_requests: {upload_csv.relative_to(PROJECT)}")

    return out


def rollback(ts: str) -> None:
    print(f"=== Rollback to TS={ts} ===")
    manifest_path = SNAP_DIR / f"manifest_{ts}.json"
    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}")
        sys.exit(1)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["items"]:
        src = PROJECT / item["dst"]
        dst = PROJECT / item["src"]
        if not src.exists():
            print(f"  [WARN] missing snapshot: {src}")
            continue
        copy2(src, dst)
        print(f"  restored: {dst.relative_to(PROJECT)}")
    print()
    print("注意: GAS スプレッドシートは Sheets UI のバージョン履歴で手動復元してください")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("workbook", nargs="?", type=Path)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--rollback", help="snapshot TS")
    args = ap.parse_args()

    if args.rollback:
        rollback(args.rollback)
        return

    if not args.workbook or not args.workbook.exists():
        print("ERROR: workbook を指定してください", file=sys.stderr)
        sys.exit(1)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"=== Phase R-F: apply_editor_decisions v2 (post-review) ===")
    print(f"  TS: {ts}")
    print(f"  workbook: {args.workbook.name}")
    print(f"  mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")

    wb = openpyxl.load_workbook(args.workbook, data_only=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    # Precheck (hash 検証含む)
    meta = load_meta(wb)
    sheets_to_read = ["1_突合せ確認", "2_受領状況編集", "3_期限管理",
                       "4_新規追加済確認", "5_既存対象外化候補",
                       "7_ページ別分類確認", "8_会社基本情報修正",
                       "9_新規受領ファイル追加"]
    sheets_for_check = {n: read_sheet(wb, n) for n in sheets_to_read}
    fails, mode = precheck(meta, sheets_for_check, conn)
    print(f"\n=== Precheck ===")
    if fails:
        for f in fails:
            print(f"  [FAIL] {f}")
        if args.execute:
            print("apply 中止 (precheck 失敗)")
            sys.exit(2)
    else:
        print(f"  OK (mode={mode})")

    # Issue 2: Atomic apply
    # 1) v3 を tmp 生成 (失敗を先取り検出)
    # 2) DB transaction 内で decisions_log + 各種 UPDATE
    # 3) COMMIT
    # 4) tmp → final atomic rename
    final_v3 = None
    tmp_v3 = None

    # まず dry-run で v2_updates を取得
    dry_result = apply_v2(wb, conn, execute=False, ts=ts, mode=mode)

    if args.execute and dry_result["v2_updates"]:
        # Step 1: v3 tmp 生成 (DB 触らず先取りエラー検出)
        try:
            final_v3, tmp_v3 = generate_v3_to_tmp(dry_result["v2_updates"], ts, dry_result["wf_id"])
        except Exception as e:
            print(f"  [FATAL] v3 tmp 生成失敗、apply 中止: {e}", file=sys.stderr)
            sys.exit(3)

    # Step 2-3: DB transaction
    try:
        if args.execute:
            conn.execute("BEGIN")
        result = apply_v2(wb, conn, execute=args.execute, ts=ts, mode=mode)
        if args.execute:
            conn.commit()
    except Exception as e:
        if args.execute:
            conn.rollback()
        # Step 失敗時: tmp も削除
        if tmp_v3 and tmp_v3.exists():
            tmp_v3.unlink()
        print(f"  EXCEPTION: {e}", file=sys.stderr)
        raise

    # Step 4: tmp → final atomic rename (DB COMMIT 成功後のみ)
    if args.execute and tmp_v3 and tmp_v3.exists():
        tmp_v3.replace(final_v3)
        print(f"  v3 atomic rename: {final_v3.relative_to(PROJECT)}")

    print("\n=== Summary ===")
    for k, v in result["summary"].items():
        print(f"  {k}: {v}")

    print(f"\n=== Output ===")
    write_dryrun_csv(result, ts)

    conn.close()
    print(f"\nDone. mode={mode}")


if __name__ == "__main__":
    main()
