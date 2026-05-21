"""
sha256 ベース dedup (ラベル品質保護版)。

旧 dedup_by_actual_sha256.py の問題:
  canonical を min_pid で選ぶと、最古のレコードが「その他/不明」のままで、
  duplicate に正しいラベルが入っていてもそれを失う。

本スクリプト:
  1. 同 cid + 同 sha256 ファイル群の各 page_no について全候補を集める
  2. ラベル品質スコアでベストを選択
  3. ベスト以外を削除 (手動ラベルは絶対保護)

ラベル品質スコア:
  + 100  manual ラベル (web_viewer/user_visual/masaru_manual)
  +  50  doc_type_secondary あり
  +  30  doc_type known specific (建設業許可証/決算書/工事経歴書/取引先一覧表/労働安全衛生誓約書/資格略字一覧/労働者名簿/会社案内)
  +  10  doc_type = 取引申請書 (filename ベースデフォルトの可能性)
  +   5  doc_type = 空白
  +   1  doc_type = その他/不明
  +   0  doc_type 空

Usage:
  python scripts/dedup_smart_label_aware.py --dry-run
  python scripts/dedup_smart_label_aware.py --execute
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

KNOWN_DOC_TYPES = {
    "建設業許可証", "決算書", "工事経歴書", "取引先一覧表",
    "労働安全衛生誓約書", "資格略字一覧", "労働者名簿", "会社案内",
}


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


def label_quality_score(doc_type, secondary, has_manual):
    if has_manual:
        return 100
    s = 0
    if secondary:
        s += 50
    if doc_type in KNOWN_DOC_TYPES:
        s += 30
    elif doc_type == "取引申請書":
        s += 10
    elif doc_type == "空白":
        s += 5
    elif doc_type in ("その他/不明", "その他"):
        s += 1
    return s


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
            "cid": cid, "file_name": fn, "min_pid": r["min_pid"], "n_pages": r["n_pages"]
        })
    print(f"path 未検出: {n_no_path}", file=sys.stderr)

    dup_groups = {k: v for k, v in by_hash.items() if len(v) > 1}
    print(f"重複ファイル群: {len(dup_groups)}", file=sys.stderr)

    delete_pages: list[dict] = []
    rebrand_pages: list[dict] = []
    skipped_manual = 0

    for (cid, digest), files_in_group in dup_groups.items():
        # canonical = ページ数最大 (rebrand 名統一先)
        canonical_name = max(files_in_group, key=lambda x: (x["n_pages"], -x["min_pid"]))["file_name"]
        # この group の全ページ取得 + ラベル品質スコア
        all_pages = []
        for f in files_in_group:
            for p in conn.execute(
                """SELECT p.page_id, p.page_no, p.doc_type_name, p.doc_type_secondary,
                          (SELECT 1 FROM page_doc_type_history h WHERE h.page_id=p.page_id
                           AND h.confirmed_by IN ('web_viewer','user_visual','masaru_manual')
                           LIMIT 1) AS has_manual
                   FROM pages p WHERE p.company_id=? AND p.file_name=?""",
                (cid, f["file_name"])
            ).fetchall():
                score = label_quality_score(p["doc_type_name"], p["doc_type_secondary"], p["has_manual"])
                all_pages.append({
                    "page_id": p["page_id"], "page_no": p["page_no"],
                    "doc_type": p["doc_type_name"], "secondary": p["doc_type_secondary"],
                    "has_manual": bool(p["has_manual"]),
                    "file_name": f["file_name"],
                    "score": score,
                })

        # page_no 別にベストを選ぶ
        from collections import defaultdict
        by_page_no = defaultdict(list)
        for p in all_pages:
            by_page_no[p["page_no"]].append(p)

        for pno, candidates in by_page_no.items():
            # スコア降順、tie-break: page_id 小
            candidates.sort(key=lambda x: (-x["score"], x["page_id"]))
            keeper = candidates[0]
            # rebrand: keeper の file_name を canonical に統一
            if keeper["file_name"] != canonical_name and not keeper["has_manual"]:
                rebrand_pages.append({
                    "page_id": keeper["page_id"], "cid": cid,
                    "old_file_name": keeper["file_name"],
                    "new_file_name": canonical_name,
                    "page_no": pno, "doc_type": keeper["doc_type"],
                })
            for c in candidates[1:]:
                if c["has_manual"]:
                    skipped_manual += 1
                    continue
                delete_pages.append({
                    "page_id": c["page_id"], "cid": cid, "file_name": c["file_name"],
                    "page_no": pno, "doc_type": c["doc_type"],
                    "kept_pid": keeper["page_id"],
                    "kept_file": keeper["file_name"],
                    "kept_score": keeper["score"],
                    "this_score": c["score"],
                })

    from collections import Counter
    by_cid = Counter(d["cid"] for d in delete_pages)
    print(f"\n=== 計画 ===")
    print(f"  削除予定 ページ:    {len(delete_pages)}")
    print(f"  rebrand 予定 ページ: {len(rebrand_pages)}")
    print(f"  手動ラベル保護:     {skipped_manual}")
    print(f"\n  会社別 削除 (top 15):")
    for cid, n in by_cid.most_common(15):
        print(f"    {cid}: {n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
        return

    workflow = f"dedup_smart_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
             f"smart dedup, kept_pid={d['kept_pid']} score={d['kept_score']}vs{d['this_score']}",
             workflow, str(uuid.uuid4()))
        )
        conn.execute("DELETE FROM pages WHERE page_id=?", (d["page_id"],))
        n_del += 1
    for r in rebrand_pages:
        # 同 (cid, new_file_name, page_no) で衝突したら skip
        conflict = conn.execute(
            "SELECT 1 FROM pages WHERE company_id=? AND file_name=? AND page_no=? AND page_id != ?",
            (r["cid"], r["new_file_name"], r["page_no"], r["page_id"])
        ).fetchone()
        if conflict:
            continue
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no, old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
            (r["page_id"], r["cid"], r["old_file_name"], r["page_no"], r["doc_type"], r["doc_type"],
             f"rebrand to canonical={r['new_file_name'][:40]}", workflow, str(uuid.uuid4()))
        )
        conn.execute("UPDATE pages SET file_name=? WHERE page_id=?", (r["new_file_name"], r["page_id"]))
        n_re += 1
    conn.commit()
    print(f"\n=== 完了 ===")
    print(f"  DELETE:  {n_del}")
    print(f"  REBRAND: {n_re}")
    print(f"  workflow: {workflow}")


if __name__ == "__main__":
    main()
