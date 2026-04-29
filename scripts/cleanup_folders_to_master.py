"""
Stage 2 Phase 2.3: data/originals/ を「マスタ正本」に揃えるフォルダ統合スクリプト。

処理:
  A. 旧字フォルダ (C0171/C0170/C0159) → 新字 DB登録フォルダの内容統合 (move)
  B. フォルダ rename (DB official_name に合わせる、最大 5 件)
  C. 空殻フォルダ archive 退避 (3 件)

src フォルダ名は scan で動的解決。想定 1 件でなければ abort。

dry-run: move_plan JSON 生成 + 検算予測のみ
execute: 各操作を順次実行、executed_at 更新、各ステップで sha256 検証
rollback: --rollback-from <move_plan.json> で逆順実行

Usage:
  python scripts/cleanup_folders_to_master.py --state-json data/stage2_state_*.json
  python scripts/cleanup_folders_to_master.py --state-json data/stage2_state_*.json --execute
  python scripts/cleanup_folders_to_master.py --rollback-from data/originals_move_plan_*.json --execute
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
ORIGINALS = PROJECT / "data" / "originals"
ARCHIVE = ORIGINALS / "_archive_20260427"

# (src_company_id, dst_company_id) - 旧字フォルダから新字フォルダへの内容統合
FOLDER_MERGES = [
    ("C0171", "C0013"),
    ("C0170", "C0051"),
    ("C0159", "C0057"),
]

# (company_id, new_folder_name)
FOLDER_RENAMES = [
    ("C0013", "C0013_吉田電氣工事株式会社"),
    ("C0051", "C0051_株式会社横河システム建築"),
    ("C0057", "C0057_豊銕工業株式会社"),
    ("C0096", "C0096_有限会社尾﨑鋼業"),  # フォルダがあれば (skip_if_none)
    ("C0048", "C0048_株式会社山西"),
    ("C0074", "C0074_株式会社ハシモト電気"),
]

# archive 対象 (FOLDER_MERGES の src_id と同じ)
ARCHIVE_IDS = ["C0171", "C0170", "C0159"]


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for ch in iter(lambda: f.read(65536), b""):
            h.update(ch)
    return h.hexdigest()


def resolve_folder(company_id: str) -> Path | None:
    """company_id prefix で実フォルダを動的解決 (想定 1 件でなければ abort)"""
    candidates = [p for p in ORIGINALS.glob(f"{company_id}_*")
                  if p.is_dir() and not p.name.startswith(f"{company_id}_archive")
                  and "_archive_" not in str(p.parent.name)]
    # _archive_20260427/ 配下は除外
    candidates = [p for p in candidates if "_archive_" not in str(p.parent.name)]
    if len(candidates) == 0:
        return None
    if len(candidates) > 1:
        raise RuntimeError(f"{company_id}: 複数フォルダ該当: {[c.name for c in candidates]}")
    return candidates[0]


def find_unique_dst(dst: Path) -> tuple[Path, bool]:
    """衝突時は _rec002, _rec003 サフィックス"""
    if not dst.exists():
        return dst, False
    stem, ext = dst.stem, dst.suffix
    for n in range(2, 1000):
        cand = dst.parent / f"{stem}_rec{n:03d}{ext}"
        if not cand.exists():
            return cand, True
    raise RuntimeError(f"衝突回避失敗: {dst}")


def build_move_plan() -> list[dict]:
    """move plan を生成 (実行はしない)"""
    plan = []

    # Phase A: 旧字フォルダの内容を新字フォルダへ move
    for src_id, dst_id in FOLDER_MERGES:
        src_folder = resolve_folder(src_id)
        dst_folder = resolve_folder(dst_id)
        if src_folder is None:
            print(f"  [SKIP merge] {src_id}: フォルダなし")
            continue
        if dst_folder is None:
            raise RuntimeError(f"merge dst {dst_id} のフォルダが見つかりません")

        for sf in src_folder.rglob("*"):
            if sf.is_file():
                rel = sf.relative_to(src_folder)
                dst_file = dst_folder / rel
                final_dst, collision = find_unique_dst(dst_file)
                plan.append({
                    "op": "move",
                    "src": str(sf),
                    "dst": str(final_dst),
                    "sha256_src": sha256_file(sf),
                    "collision_suffix": collision,
                    "executed_at": None,
                })

    # Phase B: フォルダ rename
    for cid, new_name in FOLDER_RENAMES:
        src = resolve_folder(cid)
        if src is None:
            print(f"  [SKIP rename] {cid}: フォルダなし")
            continue
        dst = src.parent / new_name
        if src == dst:
            print(f"  [SKIP rename] {cid}: 既に正しい名前")
            continue
        plan.append({
            "op": "rename_dir",
            "src": str(src),
            "dst": str(dst),
            "executed_at": None,
        })

    # Phase C: 空殻フォルダ archive 退避
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    for cid in ARCHIVE_IDS:
        src = resolve_folder(cid)
        if src is None:
            continue  # 既に move 済みで存在しないかも
        dst = ARCHIVE / src.name
        plan.append({
            "op": "archive",
            "src": str(src),
            "dst": str(dst),
            "executed_at": None,
        })

    return plan


def execute_plan(plan: list[dict]) -> list[str]:
    """plan を順次実行。失敗時は完了済み操作のリストを返す"""
    errors = []
    for i, step in enumerate(plan):
        try:
            src = Path(step["src"])
            dst = Path(step["dst"])
            if step["op"] == "move":
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
                # sha256 検証
                if "sha256_src" in step:
                    actual = sha256_file(dst)
                    if actual != step["sha256_src"]:
                        errors.append(f"{i}: sha256 不一致 src={step['sha256_src'][:8]}.. dst={actual[:8]}..")
            elif step["op"] == "rename_dir":
                src.rename(dst)
            elif step["op"] == "archive":
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
            step["executed_at"] = datetime.now().isoformat()
        except Exception as e:
            errors.append(f"step {i} {step['op']} 失敗: {e}")
            break
    return errors


def rollback_plan(plan: list[dict]) -> list[str]:
    """executed_at 済みの操作を逆順で逆操作"""
    errors = []
    executed = [s for s in plan if s.get("executed_at")]
    for step in reversed(executed):
        try:
            src = Path(step["src"])  # 元 path
            dst = Path(step["dst"])  # 移動先
            if step["op"] in ("move", "archive"):
                src.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(dst), str(src))
            elif step["op"] == "rename_dir":
                dst.rename(src)
            step["executed_at"] = None  # rollback marker
        except Exception as e:
            errors.append(f"rollback {step['op']} 失敗: {e}")
    return errors


def build_manifest(originals_dir: Path) -> dict:
    """全 originals 配下の {rel_path: {sha256, size, mtime}}"""
    m = {}
    for p in originals_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(originals_dir).as_posix()
            st = p.stat()
            m[rel] = {"sha256": sha256_file(p), "size": st.st_size, "mtime": st.st_mtime}
    return m


def verify_no_loss(before: dict, after: dict) -> list[str]:
    """multiset 検算: file_count + Counter(sha256, size)"""
    errors = []
    if len(before) != len(after):
        errors.append(f"file_count: before={len(before)}, after={len(after)}")

    bef_set = Counter((v["sha256"], v["size"]) for v in before.values())
    aft_set = Counter((v["sha256"], v["size"]) for v in after.values())
    if bef_set != aft_set:
        diff_lost = bef_set - aft_set
        diff_gained = aft_set - bef_set
        if diff_lost:
            errors.append(f"消失ファイル (multiset): {dict(diff_lost)}")
        if diff_gained:
            errors.append(f"増加ファイル (multiset): {dict(diff_gained)}")
    return errors


def detect_duplicate_folders() -> list[str]:
    """同一 company_id で複数フォルダがあれば warn"""
    cid_count: dict[str, list[str]] = {}
    for p in ORIGINALS.iterdir():
        if p.is_dir() and p.name.startswith("C") and "_" in p.name:
            cid = p.name.split("_", 1)[0]
            cid_count.setdefault(cid, []).append(p.name)
    return [f"{cid}: {names}" for cid, names in cid_count.items() if len(names) > 1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-json", type=Path, required=False)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--rollback-from", type=Path, default=None,
                    help="既存 move_plan JSON から逆順実行")
    args = ap.parse_args()

    is_dry_run = not args.execute
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.rollback_from:
        # rollback モード
        plan = json.loads(args.rollback_from.read_text(encoding="utf-8"))
        print(f"=== ROLLBACK モード ({args.rollback_from.name}) ===")
        if is_dry_run:
            for s in reversed([s for s in plan if s.get("executed_at")]):
                print(f"  逆操作 [{s['op']}] {s['dst']} → {s['src']}")
            print(f"\n※ dry-run 完了。本実行は --execute を付けてください。")
            return
        errors = rollback_plan(plan)
        if errors:
            print("\n!! rollback エラー:", file=sys.stderr)
            for e in errors:
                print(f"  - {e}", file=sys.stderr)
            sys.exit(2)
        print(f"\nrollback 完了。")
        return

    # 通常モード (move plan 生成 + dry-run/execute)
    print(f"=== Phase 2.3 originals フォルダ統合 ({'DRY-RUN' if is_dry_run else 'EXECUTE'}) ===\n")

    manifest_before = build_manifest(ORIGINALS)
    print(f"manifest_before: {len(manifest_before)} files")

    plan = build_move_plan()
    print(f"\nmove plan: {len(plan)} 操作")
    op_counter = Counter(s["op"] for s in plan)
    for op, n in op_counter.items():
        print(f"  {op}: {n} 件")

    plan_path = PROJECT / "data" / f"originals_move_plan_{ts}.json"
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2),
                         encoding="utf-8")
    print(f"\nmove plan saved: {plan_path.name}")

    if is_dry_run:
        print("\n--- dry-run 操作詳細 ---")
        for i, s in enumerate(plan[:30]):
            print(f"  [{i:3}] {s['op']:<12} {Path(s['src']).name} → {Path(s['dst']).name}"
                  + (" [COLLISION]" if s.get("collision_suffix") else ""))
        if len(plan) > 30:
            print(f"  ... 他 {len(plan) - 30} 件")
        print(f"\n※ dry-run 完了。本実行は --execute を付けてください。")

        # state JSON 更新
        if args.state_json:
            state = json.loads(args.state_json.read_text(encoding="utf-8"))
            state["phases"]["2.3_originals"]["dry_run_log"] = {
                "plan_path": str(plan_path),
                "op_counts": dict(op_counter),
                "total_ops": len(plan),
                "manifest_before_count": len(manifest_before),
            }
            args.state_json.write_text(
                json.dumps(state, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        return

    # EXECUTE
    print("\n--- 実 move 開始 ---")
    errors = execute_plan(plan)
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2),
                         encoding="utf-8")  # executed_at 更新を保存

    if errors:
        print("\n!! 実行エラー:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        print("\n部分実行状態。rollback を検討してください:")
        print(f"  python scripts/cleanup_folders_to_master.py --rollback-from {plan_path} --execute")
        sys.exit(2)

    # 検算
    manifest_after = build_manifest(ORIGINALS)
    verify_path = PROJECT / "data" / f"originals_verify_{ts}.json"
    verify_errors = verify_no_loss(manifest_before, manifest_after)
    dup = detect_duplicate_folders()

    verify_path.write_text(json.dumps({
        "manifest_before_count": len(manifest_before),
        "manifest_after_count": len(manifest_after),
        "verify_errors": verify_errors,
        "duplicate_folders": dup,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n=== 検算 ===")
    print(f"  before: {len(manifest_before)} files")
    print(f"  after:  {len(manifest_after)} files")
    print(f"  消失検知エラー: {len(verify_errors)} 件")
    print(f"  重複フォルダ:   {len(dup)} 件")
    print(f"  verify_log: {verify_path.name}")

    if verify_errors or dup:
        for e in verify_errors:
            print(f"  - {e}", file=sys.stderr)
        for d in dup:
            print(f"  - dup: {d}", file=sys.stderr)
        sys.exit(2)

    # state JSON 更新
    if args.state_json:
        state = json.loads(args.state_json.read_text(encoding="utf-8"))
        state["phases"]["2.3_originals"]["completed_at"] = datetime.now().isoformat()
        state["phases"]["2.3_originals"]["moves_applied"] = len(plan)
        state["phases"]["2.3_originals"]["move_plan_path"] = str(plan_path)
        args.state_json.write_text(
            json.dumps(state, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    print(f"\n✓ Phase 2.3 完了")


if __name__ == "__main__":
    main()
