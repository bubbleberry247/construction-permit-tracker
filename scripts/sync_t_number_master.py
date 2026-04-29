"""
T番号マスタ (基幹サーバ) → companies.t_number 同期。

入力: C:/ProgramData/RK10/Robots/建設業許可証管理/t_number_master.txt
  CSV (header: t_number, vendor_name)

照合方針:
  1. official_name の正規化マッチ (株式会社/有限会社等を除いた core 部分)
  2. name_aliases (pipe区切り) のいずれかの aggressive マッチ
  3. T番号マスタにあるが当方未マッチの社 → 「マスタ追加候補」として CSV 出力

Usage:
  python scripts/sync_t_number_master.py            # dry-run (マッチ集計のみ)
  python scripts/sync_t_number_master.py --execute  # companies.t_number を更新
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_master_db_reconcile import normalize, normalize_aggressive

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
T_MASTER = Path("C:/ProgramData/RK10/Robots/建設業許可証管理/t_number_master.txt")


def load_t_master(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def build_company_index(conn: sqlite3.Connection) -> dict:
    """正規化社名 → company_id のマップ作成。aliases も展開。"""
    idx_strict = {}
    idx_loose = {}
    for r in conn.execute(
        "SELECT company_id, official_name, name_aliases FROM companies"
    ):
        cid, off, aliases = r
        names = [off]
        if aliases:
            names.extend([a.strip() for a in aliases.split("|") if a.strip()])
        for name in names:
            ns = normalize(name)
            if ns and ns not in idx_strict:
                idx_strict[ns] = cid
            na = normalize_aggressive(name)
            if na and na not in idx_loose:
                idx_loose[na] = cid
    return idx_strict, idx_loose


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    print(f"=== T番号マスタ同期 (mode={'EXECUTE' if args.execute else 'DRY-RUN'}) ===\n")

    t_rows = load_t_master(T_MASTER)
    print(f"T番号マスタ: {len(t_rows)} 件")

    conn = sqlite3.connect(str(DB))
    idx_strict, idx_loose = build_company_index(conn)
    print(f"当方 companies: {conn.execute('SELECT COUNT(*) FROM companies').fetchone()[0]} 社\n")

    # 既に t_number あり/なし
    has_t = {r[0]: r[1] for r in conn.execute(
        "SELECT company_id, t_number FROM companies WHERE t_number IS NOT NULL AND t_number != ''"
    )}
    print(f"既に t_number 設定済: {len(has_t)} 社\n")

    matched_strict = []
    matched_loose = []
    unmatched = []  # T番号マスタにあるが当方未マッチ
    for tr in t_rows:
        tnum = tr["t_number"].strip()
        name = tr["vendor_name"].strip()
        if not tnum or not name:
            continue
        ns = normalize(name)
        na = normalize_aggressive(name)
        cid = idx_strict.get(ns) or idx_loose.get(na)
        if cid:
            if idx_strict.get(ns):
                matched_strict.append((cid, tnum, name))
            else:
                matched_loose.append((cid, tnum, name))
        else:
            unmatched.append((tnum, name))

    print(f"マッチ結果:")
    print(f"  完全一致 (normalize): {len(matched_strict)}")
    print(f"  近似一致 (aggressive): {len(matched_loose)}")
    print(f"  T番号マスタ未マッチ: {len(unmatched)}")
    print()

    # 当方の cid 単位で集計（重複マッチ防止）
    cid_to_tnum = {}
    for cid, tnum, name in matched_strict + matched_loose:
        if cid in cid_to_tnum:
            continue  # 最初のマッチを優先
        cid_to_tnum[cid] = (tnum, name)
    print(f"ユニーク cid マッチ: {len(cid_to_tnum)} 社")
    print()

    # 既存 t_number との衝突チェック
    conflicts = []
    for cid, (tnum, _) in cid_to_tnum.items():
        if cid in has_t and has_t[cid] != tnum:
            conflicts.append((cid, has_t[cid], tnum))
    if conflicts:
        print(f"⚠️ 既存 t_number と衝突: {len(conflicts)} 件")
        for cid, old, new in conflicts[:10]:
            print(f"  {cid} | 既存: {old} → 新: {new}")
        print()

    # 未マッチ T番号 (上位 15 表示)
    if unmatched:
        print(f"=== T番号マスタ未マッチ 先頭 20 ===")
        for tnum, name in unmatched[:20]:
            print(f"  {tnum} | {name}")
        print()

    if not args.execute:
        print("[DRY-RUN] companies は変更されません。--execute で更新。")
        return

    # 実行
    conn.execute("BEGIN")
    updated = 0
    for cid, (tnum, _) in cid_to_tnum.items():
        conn.execute(
            "UPDATE companies SET t_number=?, updated_at=datetime('now','localtime') WHERE company_id=?",
            (tnum, cid),
        )
        updated += 1
    conn.commit()
    print(f"✓ commit: companies.t_number UPDATE {updated} 件")

    # 未マッチ CSV 出力
    if unmatched:
        out_csv = PROJECT / "output" / "t_master_unmatched.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        with out_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_number", "vendor_name"])
            w.writerows(unmatched)
        print(f"未マッチ T番号: {out_csv}")


if __name__ == "__main__":
    main()
