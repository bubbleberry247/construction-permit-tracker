"""
Stage 4.4: 分類 CSV を読んで承認分を originals に move + DB INSERT。

approved=Y のレコードを処理。UNMATCHED や approved=空 は data/inbox_unclassified_<TS>/ に move。

dry-run / execute 両対応。move_plan JSON で rollback 可能。

Usage:
  python scripts/import_classified_inbox.py --csv data/stage4_classification_<TS>.csv
  python scripts/import_classified_inbox.py --csv data/stage4_classification_<TS>.csv --execute
  python scripts/import_classified_inbox.py --rollback-from data/inbox_move_plan_<TS>.json --execute
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
ORIGINALS = PROJECT / "data" / "originals"

CATEGORY_FOLDER = {
    "取引申請書": "01_取引申請書",
    "建設業許可証": "02_建設業許可証",
    "決算書": "03_決算書",
    "工事経歴書": "04_工事経歴書",
    "労働安全衛生誓約書": "05_労働安全衛生誓約書",
    "資格略字一覧": "06_資格略字一覧",
    "取引先一覧表": "07_取引先一覧表",
    "労働者名簿": "08_労働者名簿",
}
DEFAULT_CATEGORY = "99_受領バンドル"


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(65536), b""):
            h.update(c)
    return h.hexdigest()


def find_company_folder(cid: str) -> Path | None:
    candidates = [p for p in ORIGINALS.glob(f"{cid}_*")
                  if p.is_dir() and "_archive_" not in str(p.parent.name)]
    if len(candidates) == 0:
        return None
    if len(candidates) > 1:
        raise RuntimeError(f"{cid}: 複数候補 {[c.name for c in candidates]}")
    return candidates[0]


def find_unique_dst(dst: Path) -> tuple[Path, bool]:
    if not dst.exists():
        return dst, False
    stem, ext = dst.stem, dst.suffix
    for n in range(2, 1000):
        cand = dst.parent / f"{stem}_rec{n:03d}{ext}"
        if not cand.exists():
            return cand, True
    raise RuntimeError(f"衝突回避失敗: {dst}")


def determine_year(filename: str) -> str:
    """ファイル名先頭の YYYYMMDD から受領年を抽出"""
    import re
    m = re.match(r"^(\d{4})\d{4}_", filename)
    if m:
        return m.group(1)
    return "2026"


def build_plan(rows: list[dict], unclassified_dir: Path) -> list[dict]:
    """move plan を生成"""
    plan = []
    seen_messages = {}  # (sender, received_at) -> message_id（同セッション内）

    for row in rows:
        src_path = PROJECT / row["path"]
        if not src_path.exists():
            print(f"  [WARN] not found: {row['path']}")
            continue

        if row["approved"] != "Y" or not row["company_id"]:
            # archive 退避
            dst = unclassified_dir / src_path.name
            dst, collision = find_unique_dst(dst)
            plan.append({
                "op": "archive",
                "src": str(src_path),
                "dst": str(dst),
                "collision_suffix": collision,
                "reason": f"{row['confidence']}/approved={row['approved']}/cid={row['company_id']}",
                "executed_at": None,
            })
            continue

        # company folder 解決
        cid = row["company_id"]
        cfolder = find_company_folder(cid)
        if cfolder is None:
            # 存在しない: 新規作成 (後で execute 時に作成)
            new_folder_name = f"{cid}_{row['official_name']}"
        else:
            new_folder_name = cfolder.name

        cat = row["doc_type"] or DEFAULT_CATEGORY
        folder_cat = CATEGORY_FOLDER.get(cat, DEFAULT_CATEGORY)
        year = determine_year(src_path.name)
        dst_dir = ORIGINALS / new_folder_name / folder_cat / year
        dst = dst_dir / src_path.name
        dst, collision = find_unique_dst(dst)

        plan.append({
            "op": "import",
            "src": str(src_path),
            "dst": str(dst),
            "company_id": cid,
            "official_name": row["official_name"],
            "doc_type": cat if cat in CATEGORY_FOLDER else "",  # REQUIRED_DOCS のみ pages へ
            "collision_suffix": collision,
            "executed_at": None,
        })

    return plan


def execute_plan(plan: list[dict], conn: sqlite3.Connection):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    seen_msgs = {}  # (cid) -> message_id (本実行は単純化、cid 単位で 1 メッセージにまとめる)
    stats = {"import": 0, "archive": 0, "files": 0, "pages": 0, "msgs": 0}

    for step in plan:
        try:
            src = Path(step["src"])
            dst = Path(step["dst"])
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            step["executed_at"] = datetime.now().isoformat()

            if step["op"] == "archive":
                stats["archive"] += 1
                continue

            # import: DB INSERT
            cid = step["company_id"]
            fhash = sha256_file(dst)
            fsize = dst.stat().st_size

            # message: cid 単位 + 既存 inbound_messages あれば再利用
            msg_id = seen_msgs.get(cid)
            if not msg_id:
                # 既存の最新 inbound_messages を再利用 (重複防止)
                r = conn.execute(
                    "SELECT message_id FROM inbound_messages WHERE company_id=? "
                    "ORDER BY received_at DESC LIMIT 1", (cid,),
                ).fetchone()
                if r:
                    msg_id = r[0]
                else:
                    msg_id = str(uuid.uuid4())
                    conn.execute(
                        "INSERT INTO inbound_messages (message_id, company_id, sender_email, original_sender, received_at, created_at) "
                        "VALUES (?,?,?,?,?,?)",
                        (msg_id, cid, "stage4_import", "stage4_import", now, now),
                    )
                    stats["msgs"] += 1
                seen_msgs[cid] = msg_id

            # files INSERT
            cur = conn.execute(
                "INSERT INTO files (message_id, company_id, file_name, file_hash, file_size_bytes, "
                "saved_path, new_filename, new_path, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (msg_id, cid, src.name, fhash, fsize, step["src"], dst.name, str(dst), now),
            )
            file_id = cur.lastrowid
            stats["files"] += 1

            # pages INSERT (doc_type が REQUIRED にある場合)
            # UNIQUE INDEX (cid, file_name, page_no) 違反時は既存維持（手動編集保護）、file_id/file_hash のみ補完
            if step["doc_type"]:
                conn.execute(
                    "INSERT INTO pages (file_id, company_id, file_name, file_hash, page_no, "
                    "doc_type_name, confidence, rotation, created_at) VALUES (?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT (company_id, file_name, page_no) DO UPDATE SET "
                    "  file_id = COALESCE(pages.file_id, excluded.file_id), "
                    "  file_hash = COALESCE(pages.file_hash, excluded.file_hash)",
                    (file_id, cid, dst.name, fhash, 1, step["doc_type"], 1.0, 0, now),
                )
                stats["pages"] += 1

            stats["import"] += 1
        except Exception as e:
            print(f"  [FAIL] {step.get('op')} {step.get('src')}: {e}", file=sys.stderr)
            raise

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--rollback-from", type=Path)
    args = ap.parse_args()

    is_dry_run = not args.execute
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    unclassified = PROJECT / "data" / f"inbox_unclassified_{ts}"

    if args.rollback_from:
        plan = json.loads(args.rollback_from.read_text(encoding="utf-8"))
        print(f"=== ROLLBACK ({args.rollback_from.name}) ===")
        executed = [s for s in plan if s.get("executed_at")]
        if is_dry_run:
            for s in reversed(executed):
                print(f"  逆操作 {s['op']}: {s['dst']} → {s['src']}")
            print("\n--execute で実行")
            return
        for s in reversed(executed):
            try:
                src = Path(s["src"])  # 元 path
                dst = Path(s["dst"])  # 移動先
                src.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(dst), str(src))
                s["executed_at"] = None
            except Exception as e:
                print(f"  [FAIL rollback] {e}", file=sys.stderr)
        print("rollback 完了")
        return

    if not args.csv or not args.csv.exists():
        print("ERROR: --csv が必要", file=sys.stderr)
        sys.exit(2)

    with open(args.csv, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"読み込み: {len(rows)} rows from {args.csv.name}")

    if not is_dry_run:
        unclassified.mkdir(parents=True, exist_ok=True)

    plan = build_plan(rows, unclassified)
    plan_path = PROJECT / "data" / f"inbox_move_plan_{ts}.json"
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"move plan: {plan_path.name} ({len(plan)} ops)")

    op_cnt = {"import": 0, "archive": 0}
    for s in plan:
        op_cnt[s["op"]] = op_cnt.get(s["op"], 0) + 1
    print(f"  import: {op_cnt['import']}")
    print(f"  archive: {op_cnt['archive']}")

    if is_dry_run:
        print("\n[DRY-RUN] 本実行は --execute オプションを付けてください")
        return

    # EXECUTE
    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA foreign_keys = OFF")  # 既存62件あり
    try:
        conn.execute("BEGIN IMMEDIATE")
        stats = execute_plan(plan, conn)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n=== execute 完了 ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"\nrollback コマンド:")
    print(f"  python scripts/import_classified_inbox.py --rollback-from {plan_path} --execute")


if __name__ == "__main__":
    main()
