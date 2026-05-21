"""
全社で「実ファイル sha256」ベースに dedup する完全版。

既存の dedup_pages.py / dedup_pages_by_hash.py は file_hash 列に依存していたため、
P1 にだけ hash がある二重登録ファイルを完全には除去できなかった (C0028 など)。

本スクリプト:
  1. 全 pages から (company_id, file_name) ユニーク取得
  2. 各ファイル実体の sha256 を計算
  3. 同 cid + 同 sha256 が複数 file_name → canonical (最古 page_id) 残し他を全 page 削除
  4. 手動ラベル絶対保護

Usage:
  python scripts/dedup_by_actual_sha256.py --dry-run
  python scripts/dedup_by_actual_sha256.py --execute
"""
from __future__ import annotations

import argparse
import hashlib
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
ORIGINALS = PROJECT / "data" / "originals"
sys.stdout.reconfigure(encoding="utf-8")


def file_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def find_actual_path(file_name: str, cid: str) -> Path | None:
    for c in ORIGINALS.glob(f"{cid}_*"):
        for p in c.rglob(file_name):
            if p.is_file():
                return p
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr); sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # 全 (cid, file_name) リスト
    files = list(conn.execute(
        "SELECT company_id, file_name, MIN(page_id) min_pid, COUNT(*) n_pages FROM pages GROUP BY company_id, file_name"
    ).fetchall())
    print(f"全 ファイル: {len(files)}", file=sys.stderr)

    # 実体 sha256 計算
    by_hash: dict[tuple[str, str], list[dict]] = {}
    n_no_path = 0
    for r in files:
        cid = r["company_id"]
        fn = r["file_name"]
        p = find_actual_path(fn, cid)
        if p is None:
            n_no_path += 1
            continue
        try:
            digest = file_sha256(p)
        except Exception:
            continue
        by_hash.setdefault((cid, digest), []).append({
            "cid": cid, "file_name": fn, "min_pid": r["min_pid"], "n_pages": r["n_pages"], "path": str(p)
        })
    print(f"path 未検出: {n_no_path}", file=sys.stderr)

    # 重複グループ
    dup_groups = {k: v for k, v in by_hash.items() if len(v) > 1}
    print(f"重複ファイル群: {len(dup_groups)}", file=sys.stderr)

    # 各グループで canonical 決定 + 削除候補
    # canonical: ページ数最大 → タイブレーク min_pid 小
    # canonical に欠けているページが duplicate にあれば、file_name を rebrand して保持
    delete_pages: list[dict] = []
    rebrand_pages: list[dict] = []
    skipped_manual = 0
    for (cid, digest), files_in_group in dup_groups.items():
        canonical = max(files_in_group, key=lambda x: (x["n_pages"], -x["min_pid"]))
        canonical_pages = {
            r["page_no"] for r in conn.execute(
                "SELECT page_no FROM pages WHERE company_id=? AND file_name=?",
                (cid, canonical["file_name"])
            ).fetchall()
        }
        for f in files_in_group:
            if f["file_name"] == canonical["file_name"]:
                continue
            for p in conn.execute(
                "SELECT page_id, page_no, doc_type_name FROM pages WHERE company_id=? AND file_name=?",
                (cid, f["file_name"])
            ).fetchall():
                manual = conn.execute(
                    """SELECT 1 FROM page_doc_type_history WHERE page_id=?
                       AND confirmed_by IN ('web_viewer','user_visual','masaru_manual') LIMIT 1""",
                    (p["page_id"],)).fetchone()
                if manual:
                    skipped_manual += 1
                    continue
                if p["page_no"] in canonical_pages:
                    delete_pages.append({
                        "page_id": p["page_id"], "cid": cid, "file_name": f["file_name"],
                        "page_no": p["page_no"], "doc_type": p["doc_type_name"],
                        "canonical": canonical["file_name"],
                    })
                else:
                    rebrand_pages.append({
                        "page_id": p["page_id"], "cid": cid,
                        "old_file_name": f["file_name"],
                        "new_file_name": canonical["file_name"],
                        "page_no": p["page_no"],
                        "doc_type": p["doc_type_name"],
                    })
                    canonical_pages.add(p["page_no"])  # 後続 dup の同 page_no は削除

    # 会社別集計
    from collections import Counter
    by_cid = Counter(d["cid"] for d in delete_pages)
    by_cid_re = Counter(d["cid"] for d in rebrand_pages)
    print(f"\n=== 削除候補 ===")
    print(f"  削除予定 ページ:        {len(delete_pages)}")
    print(f"  rebrand 予定 ページ:    {len(rebrand_pages)} (canonical に欠けてた page を残す)")
    print(f"  手動ラベル保護スキップ: {skipped_manual}")
    print(f"\n  会社別 削除数 (top 15):")
    for cid, n in by_cid.most_common(15):
        re_n = by_cid_re.get(cid, 0)
        print(f"    {cid}: del={n}, rebrand={re_n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
        return

    workflow = f"dedup_actual_sha256_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n[execute] workflow={workflow}", file=sys.stderr)
    conn.execute("BEGIN IMMEDIATE")
    n_del = 0
    n_re = 0
    for d in delete_pages:
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no, old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
            (d["page_id"], d["cid"], d["file_name"], d["page_no"], d["doc_type"], "(deleted_dup)",
             f"actual sha256 dup, canonical={d['canonical'][:40]}", workflow, str(uuid.uuid4()))
        )
        conn.execute("DELETE FROM pages WHERE page_id=?", (d["page_id"],))
        n_del += 1
    for r in rebrand_pages:
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no, old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
            (r["page_id"], r["cid"], r["old_file_name"], r["page_no"], r["doc_type"], r["doc_type"],
             f"rebrand to canonical={r['new_file_name'][:40]}", workflow, str(uuid.uuid4()))
        )
        conn.execute(
            "UPDATE pages SET file_name=? WHERE page_id=?",
            (r["new_file_name"], r["page_id"])
        )
        n_re += 1
    conn.commit()
    print(f"\n=== 完了 ===")
    print(f"  DELETE:  {n_del}")
    print(f"  REBRAND: {n_re}")
    print(f"  workflow: {workflow}")


if __name__ == "__main__":
    main()
