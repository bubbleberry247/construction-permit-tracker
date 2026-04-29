"""
pages テーブルの重複レコード解消。

GPT-5.5 推奨ルール:
  canonical = file_id IS NOT NULL 側（files 参照保全）
  canonical.confidence = max(old.confidence, new.confidence)
  file_id IS NULL 側を削除（tie 両方 None なら新しい pid を残す）

手順（1 トランザクション）:
  1. 事前バックアップ（SQLite .backup API）
  2. 削除対応表 CSV 出力（loser_page_id, winner_page_id, reason）
  3. BEGIN IMMEDIATE
  4. UPDATE pages SET confidence=? WHERE page_id=?  (28 件)
  5. DELETE FROM pages WHERE page_id IN (...)        (41 件)
  6. PRAGMA foreign_key_check
  7. 重複ゼロ確認
  8. commit or rollback

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


def find_dup_pairs(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute("""
        SELECT p1.page_id AS pid1, p1.file_id AS fid1, p1.confidence AS conf1,
               p1.doc_type_name AS doc1,
               p2.page_id AS pid2, p2.file_id AS fid2, p2.confidence AS conf2,
               p2.doc_type_name AS doc2,
               p1.company_id, p1.file_name, p1.page_no
        FROM pages p1 JOIN pages p2
          ON p1.company_id=p2.company_id AND p1.file_name=p2.file_name AND p1.page_no=p2.page_no
        WHERE p1.page_id < p2.page_id
    """)]


def has_history(conn: sqlite3.Connection, pid: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM page_doc_type_history WHERE page_id=? LIMIT 1", (pid,)
    ).fetchone() is not None


# 再取込で誤分類されがちな default doc_type（具体側 vs これ → 具体側を winner にする）
REINGEST_DEFAULT_DOC_TYPE = "取引申請書"


def decide(conn: sqlite3.Connection, p: dict) -> tuple[int, int, float | None, str]:
    """Return (winner_pid, loser_pid, merged_conf, reason).

    優先ルール（上から順）:
      1. history 保護: page_doc_type_history がある側を winner
      2. 両側に history がある場合は raise（人間判断必要）
      3. doc_type 不一致で片方が再取込 default なら、もう片方を winner
      4. file_id NOT NULL 側を winner（files 参照保全）
      5. 両方 NOT NULL: confidence 高い側
      6. 両方 NULL: 新しい pid
    """
    h1 = has_history(conn, p["pid1"])
    h2 = has_history(conn, p["pid2"])
    if h1 and h2 and p["doc1"] != p["doc2"]:
        # 両側に history あり + doc_type 不一致 → 人間判断必要
        raise RuntimeError(
            f"BOTH have history with mismatched doc_type: "
            f"pid1={p['pid1']} doc1='{p['doc1']}' pid2={p['pid2']} doc2='{p['doc2']}' "
            f"(cid={p['company_id']} file={p['file_name']} page={p['page_no']})"
        )
    # 両側 history ありで値同じ場合は fall-through（後続ルールで decide）
    if h1 and not h2:
        winner, loser, wc, lc = p["pid1"], p["pid2"], p["conf1"], p["conf2"]
        reason = "history protection (pid1 has page_doc_type_history)"
    elif h2 and not h1:
        winner, loser, wc, lc = p["pid2"], p["pid1"], p["conf2"], p["conf1"]
        reason = "history protection (pid2 has page_doc_type_history)"
    elif (p["doc1"] and p["doc2"] and p["doc1"] != p["doc2"]
          and p["doc1"] == REINGEST_DEFAULT_DOC_TYPE and p["doc2"] != REINGEST_DEFAULT_DOC_TYPE):
        winner, loser, wc, lc = p["pid2"], p["pid1"], p["conf2"], p["conf1"]
        reason = f"doc_type 優先 (loser doc='{REINGEST_DEFAULT_DOC_TYPE}' は再取込 default)"
    elif (p["doc1"] and p["doc2"] and p["doc1"] != p["doc2"]
          and p["doc2"] == REINGEST_DEFAULT_DOC_TYPE and p["doc1"] != REINGEST_DEFAULT_DOC_TYPE):
        winner, loser, wc, lc = p["pid1"], p["pid2"], p["conf1"], p["conf2"]
        reason = f"doc_type 優先 (loser doc='{REINGEST_DEFAULT_DOC_TYPE}' は再取込 default)"
    elif p["fid1"] is None and p["fid2"] is not None:
        winner, loser, wc, lc = p["pid2"], p["pid1"], p["conf2"], p["conf1"]
        reason = "canonical=file_id NOT NULL (pid2)"
    elif p["fid2"] is None and p["fid1"] is not None:
        winner, loser, wc, lc = p["pid1"], p["pid2"], p["conf1"], p["conf2"]
        reason = "canonical=file_id NOT NULL (pid1)"
    elif p["fid1"] is not None and p["fid2"] is not None:
        if (p["conf1"] or 0) >= (p["conf2"] or 0):
            winner, loser, wc, lc = p["pid1"], p["pid2"], p["conf1"], p["conf2"]
        else:
            winner, loser, wc, lc = p["pid2"], p["pid1"], p["conf2"], p["conf1"]
        reason = "both have file_id -> higher confidence wins"
    else:
        winner, loser, wc, lc = p["pid2"], p["pid1"], p["conf2"], p["conf1"]
        reason = "both file_id NULL -> newer pid wins"

    confs = [x for x in [wc, lc] if x is not None]
    merged = max(confs) if confs else None
    return winner, loser, merged, reason


def merge_doc_type(p: dict, winner: int) -> tuple[str | None, str | None]:
    """winner が NULL で loser に値があれば、コピーすべき loser_doc を返す。
    両側に値あり不一致なら (None, warning_msg)。warning は raise せず CSV に記録のみ。
    """
    winner_doc = p["doc1"] if winner == p["pid1"] else p["doc2"]
    loser_doc = p["doc2"] if winner == p["pid1"] else p["doc1"]
    if winner_doc is None and loser_doc is not None:
        return loser_doc, None
    if winner_doc is not None and loser_doc is not None and winner_doc != loser_doc:
        return None, (
            f"mismatch kept winner: w='{winner_doc}' l='{loser_doc}' "
            f"(cid={p['company_id']} file={p['file_name']} page={p['page_no']})"
        )
    return None, None


def merge_file_id(p: dict, winner: int) -> int | None:
    """winner.file_id が NULL で loser.file_id NOT NULL なら、コピーすべき file_id を返す。"""
    winner_fid = p["fid1"] if winner == p["pid1"] else p["fid2"]
    loser_fid = p["fid2"] if winner == p["pid1"] else p["fid1"]
    if winner_fid is None and loser_fid is not None:
        return loser_fid
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


def save_mapping(pairs_decisions: list[tuple], out_path: Path):
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["winner_page_id", "loser_page_id", "merged_confidence", "reason",
                    "company_id", "file_name", "page_no"])
        for (winner, loser, conf, reason, meta) in pairs_decisions:
            w.writerow([winner, loser, conf, reason,
                        meta["company_id"], meta["file_name"], meta["page_no"]])
    print(f"  対応表: {out_path.name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実削除（既定は dry-run）")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    pairs = find_dup_pairs(conn)
    print(f"重複ペア: {len(pairs)} 組")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    decisions = []
    updates = []  # (winner_pid, new_conf)
    doc_merges = []  # (winner_pid, new_doc_type)
    file_merges = []  # (winner_pid, new_file_id)
    history_migrations = []  # (loser_pid → winner_pid) の history page_id 移管
    mismatches = []  # 警告: winner 維持で進める不一致
    deletes = []
    for p in pairs:
        winner, loser, merged, reason = decide(conn, p)
        decisions.append((winner, loser, merged, reason, p))
        # confidence 引き上げ
        winner_conf = p["conf1"] if winner == p["pid1"] else p["conf2"]
        if merged is not None and (winner_conf is None or winner_conf < merged):
            updates.append((winner, merged))
        # doc_type マージ判定
        copy_doc, warn = merge_doc_type(p, winner)
        if copy_doc is not None:
            doc_merges.append((winner, copy_doc))
        if warn:
            mismatches.append((winner, loser, warn, p))
        # file_id マージ判定
        copy_fid = merge_file_id(p, winner)
        if copy_fid is not None:
            file_merges.append((winner, copy_fid))
        # loser に history があれば winner へ移管（FK dangling 防止）
        if has_history(conn, loser):
            history_migrations.append((loser, winner))
        deletes.append(loser)

    print(f"  削除: {len(deletes)} 件")
    print(f"  confidence 引き上げ: {len(updates)} 件")
    print(f"  doc_type マージ (winner NULL → loser値): {len(doc_merges)} 件")
    print(f"  file_id マージ (winner NULL → loser値): {len(file_merges)} 件")
    print(f"  history 移管 (loser → winner): {len(history_migrations)} 件")
    print(f"  doc_type 不一致 (winner 維持・要レビュー): {len(mismatches)} 件")
    if mismatches:
        mm_path = PROJECT / "data" / f"dedup_mismatches_{ts}.csv"
        with mm_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["winner_page_id", "loser_page_id", "warning",
                        "company_id", "file_name", "page_no",
                        "doc1", "doc2"])
            for winner, loser, warn, p in mismatches:
                w.writerow([winner, loser, warn,
                            p["company_id"], p["file_name"], p["page_no"],
                            p["doc1"], p["doc2"]])
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

    # Pre-existing FK violation count（DB には ocr_runs 等の既知孤児あり）
    fk_before = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    print(f"\n  (参考) 事前 FK 違反: {fk_before} 件（既存の孤児）")

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

        placeholders = ",".join("?" * len(deletes))
        conn.execute(f"DELETE FROM pages WHERE page_id IN ({placeholders})", deletes)
        print(f"  DELETE 完了: {len(deletes)} 件")

        # FK check: 既存違反数より増えていなければ OK
        fk_after = conn.execute("PRAGMA foreign_key_check").fetchall()
        if len(fk_after) > fk_before:
            new_violations = len(fk_after) - fk_before
            raise RuntimeError(
                f"新規 FK 違反 {new_violations} 件発生（事前 {fk_before} → 事後 {len(fk_after)}）"
            )
        print(f"  foreign_key_check: OK（違反数 {fk_before} → {len(fk_after)}、増加なし）")

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
