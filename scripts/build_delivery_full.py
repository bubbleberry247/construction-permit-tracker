"""
客先納品 zip を一発生成。

実行手順:
  1. data/originals/ から FDE_MANAGED/ にファイルを配置（build_delivery_folder.py）
  2. index.csv 等を生成（build_index_csv.py）
  3. dashboard.xlsx を FDE_MANAGED/ 内に生成（generate_partners_list.py --link-mode file）
  4. zip 化（package_delivery.py）

成果物:
  output/delivery_zips/FDE_MANAGED_YYYYMMDD.zip
    └ 解凍 → FDE_MANAGED/dashboard.xlsx を開けば〇クリックで PDF が開く

Usage:
  python scripts/build_delivery_full.py             # 全工程実行
  python scripts/build_delivery_full.py --skip-folder  # フォルダ構築をスキップ
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
SCRIPTS = PROJECT / "scripts"


def run(label: str, cmd: list[str]) -> None:
    print(f"\n{'='*60}\n  {label}\n{'='*60}")
    print(f"  $ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=str(PROJECT))
    if r.returncode != 0:
        print(f"\n[ERROR] {label} 失敗 (exit {r.returncode})", file=sys.stderr)
        sys.exit(r.returncode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-folder", action="store_true", help="build_delivery_folder.py をスキップ")
    ap.add_argument("--package-id", default=datetime.now().strftime("%Y%m%d"), help="zip ファイル名の日付部分")
    args = ap.parse_args()

    py = sys.executable

    # Step 1: フォルダ構築
    if not args.skip_folder:
        run("Step 1/4: フォルダ構築",
            [py, str(SCRIPTS / "build_delivery_folder.py"), "--execute"])

    # Step 2: index 生成
    run("Step 2/4: index.csv 生成",
        [py, str(SCRIPTS / "build_index_csv.py")])

    # Step 3: ダッシュボード生成（FDE_MANAGED/ 内に置く → file_prefix='' で相対パス）
    dashboard_out = PROJECT / "output" / "FDE_MANAGED" / "dashboard.xlsx"
    run("Step 3/4: dashboard.xlsx 生成（ファイルリンク版）",
        [py, str(SCRIPTS / "generate_partners_list.py"),
         "--link-mode", "file",
         "--output", str(dashboard_out)])

    # Step 4: zip 化
    run("Step 4/4: zip パッケージング",
        [py, str(SCRIPTS / "package_delivery.py"),
         "--package-id", args.package_id])

    zip_path = PROJECT / "output" / "delivery_zips" / f"FDE_MANAGED_{args.package_id}.zip"
    print(f"\n{'='*60}")
    print(f"  完成: {zip_path}")
    print(f"{'='*60}")
    print(f"\n客先手順:")
    print(f"  1. zip を解凍 → 任意のフォルダに 'FDE_MANAGED' 配置")
    print(f"  2. FDE_MANAGED/dashboard.xlsx を開く")
    print(f"  3. ○ セルをクリック → 該当 PDF が開く（同一フォルダ相対パス）")


if __name__ == "__main__":
    main()
