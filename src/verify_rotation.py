"""手動検証データとの照合スクリプト"""
import sys
import csv

sys.stdout.reconfigure(encoding="utf-8")

LOG_PATH = r"C:\tmp\rotation_log.csv"

with open(LOG_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print("=== 手動検証データとの照合 ===")
print()

# イシハラ(31p): 全ページ0度（回転不要）
ishihara = [r for r in rows if r["company"] == "イシハラ"]
ishihara_rotated = [r for r in ishihara if int(r["rotation_needed"]) != 0]
print(f"イシハラ ({len(ishihara)}p): 期待=全ページ0度")
if ishihara_rotated:
    print("  NG: 回転判定されたページ:")
    for r in ishihara_rotated:
        print(f'    p{r["page_no"]}: {r["rotation_needed"]}度')
else:
    print("  OK: 全ページ回転不要")
print()

# ナカミツ(16p): 工事経歴と名簿が90度回転必要
nakamitsu = [r for r in rows if r["company"] == "ナカミツホームテック"]
nakamitsu_rotated = [r for r in nakamitsu if int(r["rotation_needed"]) != 0]
print(f"ナカミツホームテック ({len(nakamitsu)}p): 期待=工事経歴と名簿が90度回転")
for r in nakamitsu_rotated:
    print(f'  p{r["page_no"]}: {r["rotation_needed"]}度')
if not nakamitsu_rotated:
    print("  NG: 回転なし（期待は90度回転あり）")
print()

# ニュー商事(19p): 工事経歴と名簿が270度回転必要
new_shoji = [r for r in rows if r["company"] == "ニュー商事株式会社"]
new_rotated = [r for r in new_shoji if int(r["rotation_needed"]) != 0]
print(f"ニュー商事 ({len(new_shoji)}p): 期待=工事経歴と名簿が270度回転")
for r in new_rotated:
    print(f'  p{r["page_no"]}: {r["rotation_needed"]}度')
if not new_rotated:
    print("  NG: 回転なし（期待は270度回転あり）")
print()

# 三信建材(40p): 全ページ0度（回転不要）
sanshin = [r for r in rows if r["company"] == "三信建材工業株式会社"]
sanshin_rotated = [r for r in sanshin if int(r["rotation_needed"]) != 0]
print(f"三信建材工業 ({len(sanshin)}p): 期待=全ページ0度")
if sanshin_rotated:
    print("  NG: 回転判定されたページ:")
    for r in sanshin_rotated:
        print(f'    p{r["page_no"]}: {r["rotation_needed"]}度')
else:
    print("  OK: 全ページ回転不要")
print()

# 全体サマリ
total = len(rows)
rotated = sum(1 for r in rows if int(r["rotation_needed"]) > 0)
print("=== 全体サマリ ===")
print(f"総ページ数: {total}")
print(f"回転したページ: {rotated}")
print(f"回転不要ページ: {total - rotated}")
