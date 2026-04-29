"""
pages テーブルの重複レコード解消（グループ単位処理、GPT-5.5 review APPROVE 済）。

優先ルール（decide_group 内、フィルタを順に適用）:
  1. history 保護: page_doc_type_history を持つ member を優先
  2. 取引申請書 default を loser 化（再取込誤分類対策）
  3. file_id NOT NULL 優先（files 参照保全）
  4. confidence 高い側 → 最新 pid

手順（1 トランザクション内）:
  1. 事前バックアップ（SQLite .backup API）
  2. 対応表 CSV 出力（winner/loser 詳細）
  3. BEGIN IMMEDIATE
  4. UPDATE confidence / doc_type / file_id（winner 側補完）
  5. UPDATE page_doc_type_history.page_id 移管（loser → winner、FK dangling 防止）
  6. DELETE FROM pages WHERE page_id IN (...)
  7. PRAGMA foreign_key_check の tuple set 差分で新規違反検出
  8. 重複ゼロ確認
  9. commit or rollback

Usage:
    python scripts/dedup_pages.py                # dry-run（既定）
    python scripts/dedup_pages.py --execute      # 実削除
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"


def find_dup_groups(conn: sqlite3.Connection) -> list[dict]:
    """同一 (company_id, file_name, page_no) を持つ pages 行をグループ化して返す。
    各グループは 2 個以上の page_id を含み、先頭から page_id 昇順。
    """
    keys = conn.execute("""
        SELECT company_id, file_name, page_no
        FROM pages
        GROUP BY company_id, file_name, page_no
        HAVING COUNT(*) > 1
    """).fetchall()
    groups = []
    for k in keys:
        members = [dict(r) for r in conn.execute(
            "SELECT page_id, file_id, confidence, doc_type_name "
            "FROM pages WHERE company_id=? AND file_name=? AND page_no=? "
            "ORDER BY page_id",
            (k["company_id"], k["file_name"], k["page_no"]),
        )]
        groups.append({
            "company_id": k["company_id"],
            "file_name": k["file_name"],
            "page_no": k["page_no"],
            "members": members,
        })
    return groups


def has_history(conn: sqlite3.Connection, pid: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM page_doc_type_history WHERE page_id=? LIMIT 1", (pid,)
    ).fetchone() is not None


# 再取込で誤分類されがちな default doc_type（具体側 vs これ → 具体側を winner にする）
REINGEST_DEFAULT_DOC_TYPE = "取引申請書"


def decide_group(conn: sqlite3.Connection, group: dict) -> tuple[dict, list[dict], str]:
    """Return (winner_member, losers_members, reason).

    優先ルール（フィルタを順に適用、最後に残った中から最良 1 件）:
      1. history 保護: history を持つ member が 1 つだけならその member が winner
         複数あって doc_type 不一致なら raise（人間判断）
         複数あって doc_type 一致なら、その中から続行
      2. doc_type 優先: 「取引申請書」(再取込 default) を持つ member は除外
         （非 default の member が他にある場合のみ）
      3. file_id NOT NULL 優先
      4. confidence 高い側
      5. 最新 pid
    """
    members = group["members"]
    cid, fname, pno = group["company_id"], group["file_name"], group["page_no"]

    # Stage 1: history 保護
    histed = [m for m in members if has_history(conn, m["page_id"])]
    if len(histed) == 1:
        winner = histed[0]
        losers = [m for m in members if m["page_id"] != winner["page_id"]]
        return winner, losers, f"history protection (only pid={winner['page_id']} has history)"
    if len(histed) >= 2:
        docs = {m["doc_type_name"] for m in histed}
        if len(docs) > 1:
            raise RuntimeError(
                f"MULTIPLE have history with mismatched doc_type: "
                f"cid={cid} file={fname} page={pno} histed_pids={[m['page_id'] for m in histed]} docs={docs}"
            )
        candidates = histed  # history 持ち同 doc_type の中から続行
        reason_prefix = "history-protected (multi, same doc_type)"
    else:
        candidates = members
        reason_prefix = ""

    # Stage 2: doc_type 優先（取引申請書 default を loser 化）
    docs_in_candidates = {c["doc_type_name"] for c in candidates if c["doc_type_name"]}
    non_default = docs_in_candidates - {REINGEST_DEFAULT_DOC_TYPE}
    if REINGEST_DEFAULT_DOC_TYPE in docs_in_candidates and non_default:
        candidates = [c for c in candidates if c["doc_type_name"] != REINGEST_DEFAULT_DOC_TYPE]
        reason_prefix = (reason_prefix + " + " if reason_prefix else "") + \
                        f"doc_type 優先 (除外: {REINGEST_DEFAULT_DOC_TYPE})"

    # Stage 3: file_id NOT NULL 優先
    has_fid = [c for c in candidates if c["file_id"] is not None]
    if has_fid:
        candidates = has_fid

    # Stage 4: confidence 高い側 / Stage 5: 最新 pid
    candidates.sort(key=lambda c: (-(c["confidence"] or 0), -c["page_id"]))
    winner = candidates[0]
    losers = [m for m in members if m["page_id"] != winner["page_id"]]
    if not reason_prefix:
        reason_prefix = "file_id/confidence/pid"
    return winner, losers, reason_prefix


def merge_doc_type_group(winner: dict, losers: list[dict]) -> tuple[str | None, str | None]:
    """winner.doc_type_name が NULL で loser に値があれば、コピーすべき値を返す。
    losers に winner と異なる doc_type があれば warning（raise しない）。
    """
    if winner["doc_type_name"] is None:
        for l in losers:
            if l["doc_type_name"] is not None:
                return l["doc_type_name"], None
        return None, None
    # winner has doc_type; check mismatch in losers
    diff_docs = [l["doc_type_name"] for l in losers
                 if l["doc_type_name"] is not None and l["doc_type_name"] != winner["doc_type_name"]]
    if diff_docs:
        return None, (
            f"mismatch kept winner: w='{winner['doc_type_name']}' losers={diff_docs}"
        )
    return None, None


def merge_file_id_group(winner: dict, losers: list[dict]) -> int | None:
    """winner.file_id が NULL で losers のいずれかに値があれば、コピーすべき file_id を返す。"""
    if winner["file_id"] is not None:
        return None
    for l in losers:
        if l["file_id"] is not None:
            return l["file_id"]
    return None


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = db_path.parent / f"permit_tracker.db.bak_pre_dedup_{ts}"
    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(bak))
    with dst:
        src.backup(dst)
    dst.close()
    src.close()
    # 検証: 開けて pages 件数が読めること
    v = sqlite3.connect(str(bak))
    cnt = v.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    v.close()
    print(f"  バックアップ: {bak.name} (pages={cnt})")
    return bak


def save_mapping(group_decisions: list[tuple], out_path: Path):
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["winner_page_id", "loser_page_id", "winner_doc_type", "loser_doc_type",
                    "winner_file_id", "loser_file_id", "winner_confidence", "loser_confidence",
                    "reason", "company_id", "file_name", "page_no", "group_size"])
        for (winner, losers, reason, group) in group_decisions:
            for loser in losers:
                w.writerow([winner["page_id"], loser["page_id"],
                            winner["doc_type_name"], loser["doc_type_name"],
                            winner["file_id"], loser["file_id"],
                            winner["confidence"], loser["confidence"],
                            reason, group["company_id"], group["file_name"], group["page_no"],
                            len(group["members"])])
    print(f"  対応表: {out_path.name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実削除（既定は dry-run）")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    groups = find_dup_groups(conn)
    total_pages = sum(len(g["members"]) for g in groups)
    print(f"重複物理ページ: {len(groups)} 件（page_id 総数 {total_pages}）")
    grp_sizes = {}
    for g in groups:
        grp_sizes[len(g["members"])] = grp_sizes.get(len(g["members"]), 0) + 1
    for sz in sorted(grp_sizes.keys()):
        print(f"  {sz}-way: {grp_sizes[sz]} 件")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    decisions = []
    updates = []  # (winner_pid, new_conf)
    doc_merges = []  # (winner_pid, new_doc_type)
    file_merges = []  # (winner_pid, new_file_id)
    history_migrations = []  # (loser_pid → winner_pid) page_doc_type_history 移管
    ocr_run_migrations = []  # (loser_pid → winner_pid) ocr_runs 移管
    mismatches = []  # 警告: winner 維持で進める不一致
    deletes = []
    winners = []  # winner page_id 一覧（assert 用）
    for g in groups:
        winner, losers, reason = decide_group(conn, g)
        decisions.append((winner, losers, reason, g))
        winners.append(winner["page_id"])
        # confidence 引き上げ: winner より高い conf を持つ loser があれば反映
        winner_conf = winner["confidence"] or 0
        max_loser_conf = max((l["confidence"] or 0) for l in losers)
        if max_loser_conf > winner_conf:
            updates.append((winner["page_id"], max_loser_conf))
        # doc_type マージ
        copy_doc, warn = merge_doc_type_group(winner, losers)
        if copy_doc is not None:
            doc_merges.append((winner["page_id"], copy_doc))
        if warn:
            mismatches.append((winner["page_id"], [l["page_id"] for l in losers], warn, g))
        # file_id マージ
        copy_fid = merge_file_id_group(winner, losers)
        if copy_fid is not None:
            file_merges.append((winner["page_id"], copy_fid))
        # loser ごとに参照テーブル移管 + delete
        for loser in losers:
            if has_history(conn, loser["page_id"]):
                history_migrations.append((loser["page_id"], winner["page_id"]))
            # ocr_runs にも page_id 参照あり → 移管対象（条件チェック含めて常時積む、UPDATE で 0 row もありえる）
            cnt = conn.execute(
                "SELECT COUNT(*) FROM ocr_runs WHERE page_id=?", (loser["page_id"],)
            ).fetchone()[0]
            if cnt > 0:
                ocr_run_migrations.append((loser["page_id"], winner["page_id"]))
            deletes.append(loser["page_id"])

    # 安全 assert (GPT-5.5 指摘): deletes に重複なし、winner と loser に重複なし
    assert len(set(deletes)) == len(deletes), \
        f"deletes に重複: {len(deletes)} != {len(set(deletes))}"
    assert set(winners).isdisjoint(deletes), \
        f"winner と loser に重複: {set(winners) & set(deletes)}"

    print(f"\n  削除: {len(deletes)} 件")
    print(f"  confidence 引き上げ: {len(updates)} 件")
    print(f"  doc_type マージ (winner NULL → loser値): {len(doc_merges)} 件")
    print(f"  file_id マージ (winner NULL → loser値): {len(file_merges)} 件")
    print(f"  history 移管 (loser → winner): {len(history_migrations)} 件")
    print(f"  ocr_runs 移管 (loser → winner): {len(ocr_run_migrations)} 件")
    print(f"  doc_type 不一致 (winner 維持・要レビュー): {len(mismatches)} 件")
    if mismatches:
        mm_path = PROJECT / "data" / f"dedup_mismatches_{ts}.csv"
        with mm_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["winner_page_id", "loser_page_ids", "warning",
                        "company_id", "file_name", "page_no"])
            for winner_pid, loser_pids, warn, g in mismatches:
                w.writerow([winner_pid, ",".join(str(x) for x in loser_pids), warn,
                            g["company_id"], g["file_name"], g["page_no"]])
        print(f"  不一致レポート: {mm_path.name}")

    # 対応表出力（dry-run でも作成）
    mapping_path = PROJECT / "data" / f"dedup_mapping_{ts}.csv"
    save_mapping(decisions, mapping_path)

    if not args.execute:
        print("\n(dry-run) 実削除しません。--execute で実行してください。")
        conn.close()
        return

    # 実行
    print("\n--- 事前バックアップ ---")
    bak_path = backup_db(DB)

    # Pre-existing FK violations を tuple set として記録（GPT-5.5 #1: 件数比較ではなく差分検出）
    fk_before = {tuple(r) for r in conn.execute("PRAGMA foreign_key_check").fetchall()}
    print(f"\n  (参考) 事前 FK 違反: {len(fk_before)} 件（既存の孤児）")

    print("\n--- トランザクション実行 ---")
    try:
        conn.execute("BEGIN IMMEDIATE")
        for pid, conf in updates:
            conn.execute("UPDATE pages SET confidence=? WHERE page_id=?", (conf, pid))
        print(f"  UPDATE confidence 完了: {len(updates)} 件")

        for pid, doc in doc_merges:
            conn.execute("UPDATE pages SET doc_type_name=? WHERE page_id=?", (doc, pid))
        print(f"  UPDATE doc_type_name 完了: {len(doc_merges)} 件")

        for pid, fid in file_merges:
            conn.execute("UPDATE pages SET file_id=? WHERE page_id=?", (fid, pid))
        print(f"  UPDATE file_id 完了: {len(file_merges)} 件")

        # history 移管: loser の page_id 参照を winner に付け替える（FK dangling 防止）
        for loser_pid, winner_pid in history_migrations:
            conn.execute(
                "UPDATE page_doc_type_history SET page_id=? WHERE page_id=?",
                (winner_pid, loser_pid),
            )
        print(f"  UPDATE page_doc_type_history.page_id 移管完了: {len(history_migrations)} 件")

        # ocr_runs 移管（FK dangling 防止）
        for loser_pid, winner_pid in ocr_run_migrations:
            conn.execute(
                "UPDATE ocr_runs SET page_id=? WHERE page_id=?",
                (winner_pid, loser_pid),
            )
        print(f"  UPDATE ocr_runs.page_id 移管完了: {len(ocr_run_migrations)} 件")

        placeholders = ",".join("?" * len(deletes))
        conn.execute(f"DELETE FROM pages WHERE page_id IN ({placeholders})", deletes)
        print(f"  DELETE 完了: {len(deletes)} 件")

        # FK check: tuple set 差分で「新規違反」を厳密検出（GPT-5.5 #1）
        fk_after = {tuple(r) for r in conn.execute("PRAGMA foreign_key_check").fetchall()}
        new_violations = fk_after - fk_before
        if new_violations:
            raise RuntimeError(
                f"新規 FK 違反 {len(new_violations)} 件発生: {list(new_violations)[:5]}"
            )
        print(f"  foreign_key_check: OK（事前 {len(fk_before)} → 事後 {len(fk_after)}、新規違反なし）")

        # 重複ゼロ確認
        remaining = conn.execute("""
            SELECT COUNT(*) FROM (
              SELECT company_id, file_name, page_no FROM pages
              GROUP BY company_id, file_name, page_no HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if remaining > 0:
            raise RuntimeError(f"重複が残っている: {remaining} 組")
        print("  重複ゼロ確認: OK")

        conn.commit()
        print("\n✓ commit 完了")
    except Exception as e:
        conn.rollback()
        print(f"\n✗ rollback: {e}", file=sys.stderr)
        print(f"  バックアップ: {bak_path}")
        raise

    # 事後確認
    total = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    print(f"\n事後: pages 件数 = {total}")
    conn.close()
    print(f"対応表: {mapping_path.name}")
    print(f"バックアップ: {bak_path.name}")


if __name__ == "__main__":
    main()
