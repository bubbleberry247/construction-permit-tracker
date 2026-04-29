"""
客先納品用フォルダ構造を構築する。

入力:
  - data/originals/C0XXX_*/*.pdf
  - data/permit_tracker.db (pages, files, companies テーブル)
  - config/required_docs.json

出力:
  output/FDE_MANAGED/
    manifest_sha256.txt
    10_originals/
      C0XXX_<会社名>/
        {prefix}_{folder}/
          {year}/
            <原本ファイル名.pdf>    ← リネーム禁止
        99_受領バンドル/
          <バンドルPDF>.pdf

ルール:
  - 原本 PDF のファイル名は変更しない（ISO 15489 真正性）
  - 分類は pages.doc_type_name を file_name ごとに集計、70% 以上で単一書類判定
  - 複数書類混在（bundle）は 99_受領バンドル/ に置く
  - 同名衝突時は _rec002, _rec003... で回避
  - 受領年は pages.created_at から推定、不明なら「受領年不明」

Usage:
  python scripts/build_delivery_folder.py               # dry-run
  python scripts/build_delivery_folder.py --execute     # 実コピー
  python scripts/build_delivery_folder.py --company C0008  # 特定社のみ
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
ORIGINALS_ROOT = PROJECT / "data" / "originals"
CONFIG = PROJECT / "config" / "required_docs.json"
OUTPUT_ROOT = PROJECT / "output" / "FDE_MANAGED"

REQUIRED_DOCS = json.loads(CONFIG.read_text(encoding="utf-8"))


def build_doc_type_folder_map() -> dict[str, tuple[int, str]]:
    """doc_type_name 文字列 → (order, folder 名) のマップ。aliases 全部対応。

    order=10,20,...,90 を 01,02,...,09 の 2 桁連番プレフィックスに変換。
    """
    m: dict[str, tuple[int, str]] = {}
    for d in REQUIRED_DOCS:
        prefix = d["order"] // 10
        folder = f"{prefix:02d}_{d['display']}"
        m[d["display"]] = (d["order"], folder)
        for a in d.get("aliases", []):
            m[a] = (d["order"], folder)
    return m


DOC_TYPE_MAP = build_doc_type_folder_map()
BUNDLE_FOLDER = "99_受領バンドル"

# fix_single_content_tags.py と同じバンドル PDF 判定パターン
# ファイル名でバンドルと判明するものは分類前に強制的に bundle 扱い
BUNDLE_PATTERN = re.compile(
    r"御?お?取引条件|継続取引|新規・継続|取引申請書類|取引条件等説明書|継続取引申請書類"
)


def parse_company_folder(name: str) -> tuple[str, str] | None:
    """'C0008_三和シャッター工業株式会社' → ('C0008', '三和シャッター工業株式会社')"""
    m = re.match(r"^(C\d{4})_(.+)$", name)
    if not m:
        return None
    return m.group(1), m.group(2)


def get_file_classification(conn: sqlite3.Connection, company_id: str, file_name: str) -> tuple[str, str]:
    """
    file_name の pages 情報から分類を決定。
    戻り値: (分類結果, 年度)
      分類結果: 'bundle' または REQUIRED_DOCS の display or 'unknown'
      年度: 'YYYY' または '受領年不明'
    """
    rows = conn.execute(
        "SELECT doc_type_name, created_at FROM pages WHERE company_id=? AND file_name=?",
        (company_id, file_name),
    ).fetchall()
    if not rows:
        return "unknown", "受領年不明"

    # ファイル名でバンドル判定されたら強制 bundle（pages 分布に依存しない）
    if BUNDLE_PATTERN.search(file_name):
        classification = "bundle"
    else:
        dt_counts = Counter(r[0] for r in rows if r[0] and r[0] not in ("その他/不明", "その他"))
        total = sum(dt_counts.values())
        if total > 0:
            (top_dt, top_cnt), *_ = dt_counts.most_common(1)
            if top_cnt / total >= 0.7 and top_dt in DOC_TYPE_MAP:
                classification = top_dt
            else:
                classification = "bundle"
        else:
            classification = "unknown"

    dates = [r[1] for r in rows if r[1]]
    year = "受領年不明"
    if dates:
        try:
            y = min(dates)[:4]
            if y.isdigit() and 2020 <= int(y) <= 2030:
                year = y
        except (ValueError, TypeError):
            pass
    return classification, year


def calc_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def dest_path_for(company_folder_name: str, classification: str, year: str, filename: str) -> Path:
    if classification in ("bundle", "unknown"):
        return OUTPUT_ROOT / "10_originals" / company_folder_name / BUNDLE_FOLDER / filename
    order, folder = DOC_TYPE_MAP[classification]
    return OUTPUT_ROOT / "10_originals" / company_folder_name / folder / year / filename


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実コピー（既定 dry-run）")
    ap.add_argument("--company", help="特定 company_id のみ処理（例 C0008）")
    args = ap.parse_args()

    if not ORIGINALS_ROOT.exists():
        print(f"ERROR: {ORIGINALS_ROOT} が存在しません", file=sys.stderr)
        sys.exit(1)

    if args.execute and OUTPUT_ROOT.exists():
        print(f"既存の {OUTPUT_ROOT} を削除します（毎回クリーン再構築）")
        shutil.rmtree(OUTPUT_ROOT)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    company_dirs = sorted(ORIGINALS_ROOT.iterdir())
    if args.company:
        company_dirs = [d for d in company_dirs if d.name.startswith(args.company + "_")]

    stats: dict[str, int] = defaultdict(int)
    manifest_entries: list[tuple[str, str]] = []
    copy_plan: list[tuple[Path, Path]] = []
    planned_keys: set[str] = set()

    for cdir in company_dirs:
        if not cdir.is_dir():
            continue
        parsed = parse_company_folder(cdir.name)
        if not parsed:
            continue
        cid, cname = parsed
        # rglob で直下 + サブディレクトリ配下も対象 (Stage 3.3/4 でカテゴリフォルダに整理した分も含む)
        # 同名ファイル (直下と sub に同じファイル) は dict で重複排除
        pdf_dict: dict[str, Path] = {}
        for p in cdir.rglob("*.pdf"):
            if p.name not in pdf_dict:
                pdf_dict[p.name] = p
        pdf_files = sorted(pdf_dict.values(), key=lambda p: p.name)
        if not pdf_files:
            stats["empty_companies"] += 1
            continue

        for pdf in pdf_files:
            classification, year = get_file_classification(conn, cid, pdf.name)
            dest = dest_path_for(cdir.name, classification, year, pdf.name)
            key = str(dest.relative_to(OUTPUT_ROOT))

            if key in planned_keys:
                n = 2
                while True:
                    alt = dest.parent / f"{dest.stem}_rec{n:03d}{dest.suffix}"
                    alt_key = str(alt.relative_to(OUTPUT_ROOT))
                    if alt_key not in planned_keys:
                        dest = alt
                        key = alt_key
                        stats["collisions"] += 1
                        break
                    n += 1

            copy_plan.append((pdf, dest))
            planned_keys.add(key)
            stats[f"class_{classification}"] += 1

    print(f"対象会社: {len(company_dirs)}")
    print(f"コピー計画: {len(copy_plan)} PDF")
    print(f"  バンドル: {stats.get('class_bundle', 0)}")
    print(f"  不明: {stats.get('class_unknown', 0)}")
    for doc in REQUIRED_DOCS:
        k = f"class_{doc['display']}"
        if stats.get(k, 0) > 0:
            print(f"  {doc['display']}: {stats[k]}")
    print(f"  衝突回避リネーム: {stats.get('collisions', 0)}")
    print(f"  PDF なし会社: {stats.get('empty_companies', 0)}")

    if not args.execute:
        print("\n=== サンプル（先頭 10 件） ===")
        for src, dest in copy_plan[:10]:
            print(f"  {src.name} -> {dest.relative_to(OUTPUT_ROOT)}")
        print("\n(dry-run) 実コピーしません。--execute で実行してください。")
        return

    print(f"\n--- 実コピー開始 ---")
    for i, (src, dest) in enumerate(copy_plan, 1):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        sha = calc_sha256(dest)
        manifest_entries.append((sha, str(dest.relative_to(OUTPUT_ROOT)).replace("\\", "/")))
        if i % 100 == 0:
            print(f"  {i}/{len(copy_plan)}")
    print(f"  {len(copy_plan)}/{len(copy_plan)} 完了")

    manifest_path = OUTPUT_ROOT / "manifest_sha256.txt"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as f:
        f.write(f"# 原本 PDF SHA256 一覧\n")
        f.write(f"# generated_at: {datetime.now().isoformat()}\n")
        f.write(f"# total: {len(manifest_entries)} files\n\n")
        for sha, rel in sorted(manifest_entries, key=lambda x: x[1]):
            f.write(f"{sha}  {rel}\n")
    print(f"\n✓ manifest_sha256.txt 生成: {manifest_path}")
    print(f"✓ 出力先: {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
