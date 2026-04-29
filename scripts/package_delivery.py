"""
output/FDE_MANAGED/ を納品 zip にパッケージング。

- zip 内ルートに README_運用規程.txt を同梱
- zip 内のフォルダ階層は FDE_MANAGED/ を保持
- パスワードなし zip
- 出力: output/delivery_zips/FDE_MANAGED_YYYYMMDD.zip

Usage:
  python scripts/package_delivery.py
  python scripts/package_delivery.py --package-id v1.0
"""
from __future__ import annotations

import argparse
import sys
import zipfile
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
FDE_MANAGED = PROJECT / "output" / "FDE_MANAGED"
ZIP_OUT_DIR = PROJECT / "output" / "delivery_zips"

README_TEXT = """\
協力会社必要書類管理システム 運用規程（客先向け）

■ 最新版のお知らせ（2026-04-27）
  本パッケージには「v2」一覧表・突合せレポートが含まれます。
  ・00_一覧表_v2_20260427.xlsx       … 最新版（マスタ145社主軸）
  ・00_突合せレポート_v2_20260427.xlsx … 最新版（aggressive マッチング）
  ・00_一覧表_20260426.xlsx          … 旧版（参照用に残置）
  ・00_突合せレポート_20260426.xlsx   … 旧版（参照用に残置）
  審査・確認は v2 を正としてご利用ください。

■ このフォルダの構成
  協力会社書類管理/
    FDE_MANAGED/          ← FDE 管理領域。更新時は毎回丸ごと差し替え
      dashboard.xlsx
      manifest_sha256.txt
      00_index/
      10_originals/
    CLIENT_LOCAL/         ← 客先編集領域。更新時も保持
      notes.xlsx          ← 客先メモ専用。自由に記入可
      backup_previous/    ← 旧 FDE_MANAGED を退避

■ 日常運用
  1. dashboard.xlsx をダブルクリックで開く（読み取り専用を推奨）
  2. Dashboard シートで会社×書類の揃い状況を確認
  3. 受領済セル（〇）をクリックすると PDF が開く
  4. 客先メモは CLIENT_LOCAL/notes.xlsx にのみ記入

■ 更新手順（新しい zip を受領したとき）

  【鉄則】既存フォルダへの「上書き解凍」はしない。退避 → 新配置。

  1. dashboard.xlsx と該当 PDF をすべて閉じる
  2. 既存の FDE_MANAGED/ フォルダを、
     CLIENT_LOCAL/backup_previous/FDE_MANAGED_YYYYMMDD/ に
     「移動（リネーム）」する（Ctrl+X → Ctrl+V）
  3. 新しい FDE_MANAGED_YYYYMMDD.zip を一時フォルダに解凍
  4. 解凍結果の FDE_MANAGED/ を 協力会社書類管理/ 直下へ配置
  5. CLIENT_LOCAL/notes.xlsx はそのまま残す（削除しない）
  6. 新しい dashboard.xlsx を開き、DeliveryLog シートで追加内容を確認

  ※ backup_previous/ は直近 3 世代まで保持を推奨（古いものは削除可）

■ 書類が見つからないとき
  1. Documents シートで original_filename / rel_path を確認
  2. Explorer で 10_originals/CXXXX_会社名/ フォルダを開く
  3. バンドル PDF は 99_受領バンドル/ にある

■ manifest_sha256.txt について
  原本 PDF の改竄検出用の SHA256 一覧。監査時以外は参照不要。

■ トラブル時の連絡先
  Config シート を参照。
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--package-id", default=f"{datetime.now().strftime('%Y%m%d')}",
                    help="パッケージ ID（日付）")
    args = ap.parse_args()

    if not FDE_MANAGED.exists():
        print(f"ERROR: {FDE_MANAGED} が存在しません", file=sys.stderr)
        sys.exit(1)

    ZIP_OUT_DIR.mkdir(parents=True, exist_ok=True)
    zip_name = f"FDE_MANAGED_{args.package_id}.zip"
    zip_path = ZIP_OUT_DIR / zip_name

    readme_path = FDE_MANAGED.parent / "README_運用規程.txt"
    readme_path.write_text(README_TEXT, encoding="utf-8")

    n_files = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(readme_path, arcname="README_運用規程.txt")
        for p in FDE_MANAGED.rglob("*"):
            if p.is_file():
                rel = p.relative_to(FDE_MANAGED.parent)  # output/FDE_MANAGED/xxx → FDE_MANAGED/xxx
                zf.write(p, arcname=str(rel))
                n_files += 1

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"✓ {zip_path}")
    print(f"  ファイル数: {n_files + 1}（README 含む）")
    print(f"  サイズ: {size_mb:.1f} MB")
    print(f"\n納品 zip 準備完了。客先 PC の 協力会社書類管理/ ルートで解凍してください。")


if __name__ == "__main__":
    main()
