# DESIGN.md — 建設業許可証管理システム (construction-permit-tracker)

> このファイルはAIエージェントが正確な日本語UIを生成するためのデザイン仕様書です。
> awesome-design-md-jp テンプレートに基づき、Notion風デザインシステムの日本語最適化仕様を定義。
> セクションヘッダーは英語、値の説明は日本語で記述しています。

---

## 1. Visual Theme & Atmosphere

- **デザイン方針**: Notion風のクリーンでミニマルな業務UI。白背景にベージュのアクセント、左ボーダーによるステータス表現
- **密度**: 中程度の情報密度。KPIカード + テーブル一覧 + 詳細パネルの3レイヤー構成
- **キーワード**: クリーン、信頼性、ミニマル、業務的、Notion風
- **特徴**: box-shadow を排し、ボーダーと背景色の透過度で奥行きを表現。フラットだが冷たくない

---

## 2. Color Palette & Roles

### Primary（ブランドカラー）

- **Primary** (`#0075de`): Notionブルー。CTAボタン、アクティブタブ、フォーカスリング
- **Primary Light** (`#3a9aec`): ライトブルー。ホバー、補助アクセント

### Semantic（意味的な色）

- **Danger** (`#e03e3e`): エラー、期限切れ、削除。背景: `rgba(255,226,221,0.5)`
- **Warning** (`#d9730d`): 警告、期限90日以内。背景: `rgba(253,236,200,0.5)`
- **Success** (`#0f7b6c`): 有効、完了。背景: `rgba(219,237,219,0.5)`

### Neutral（ニュートラル — Notion カラー体系）

- **Text Primary** (`rgba(55,53,47,0.95)`): 本文テキスト、見出し
- **Text Secondary** (`rgba(55,53,47,0.65)`): 補足テキスト、ラベル
- **Text Muted** (`rgba(55,53,47,0.45)`): 無効状態、テーブルヘッダー
- **Border** (`rgba(55,53,47,0.09)`): 区切り線、カード枠
- **Border Strong** (`rgba(55,53,47,0.16)`): 入力欄の枠
- **Background** (`#f7f6f3`): ページ背景（薄いベージュ）
- **Surface** (`#ffffff`): カード、ヘッダー、テーブル背景

### ステータスカード背景（KPI用）

- **Danger BG** (`rgba(255,226,221,0.15)`)
- **Warning BG** (`rgba(253,236,200,0.15)`)
- **Success BG** (`rgba(219,237,219,0.15)`)
- **Primary BG** (`rgba(211,229,239,0.15)`)

### 業種タグカラー

- **一般** (`rgba(211,229,239,0.5)` / `#0075de`): 一般建設業許可
- **特定** (`rgba(255,226,221,0.5)` / `#e03e3e`): 特定建設業許可
- **両方** (`rgba(232,222,238,0.5)` / `#6940a5`): 一般+特定

---

## 3. Typography Rules

### 3.1 和文フォント

- **ゴシック体**: Noto Sans JP（Google Fonts / ウェイト 400, 500, 600, 700）
- 見出し・ラベルに 500〜600、KPI数値に 700 を使用

### 3.2 欧文フォント

- **サンセリフ**: Inter（Google Fonts / ウェイト 400, 500, 600, 700）
- 数値・英字表記に優先使用。KPI数値は `font-feature-settings: "tnum"` でテーブル数字

### 3.3 font-family 指定

```css
/* 本文・UI全般 */
font-family: 'Inter', 'Noto Sans JP', -apple-system, BlinkMacSystemFont,
  'Segoe UI', 'Hiragino Kaku Gothic ProN', 'Yu Gothic', Meiryo, sans-serif;

/* 等幅（コード・ID表示） */
font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
```

**フォールバックの考え方**:
- Inter を先頭に置き、数値・英字の表示品質を確保
- Noto Sans JP で和文を高品質にカバー
- システムフォント（Hiragino / Yu Gothic / Meiryo）は Noto Sans JP 未ロード時のフォールバック
- 最後に `sans-serif`

### 3.4 文字サイズ・ウェイト階層

| Role | Font | Size | Weight | Line Height | Letter Spacing | 備考 |
|------|------|------|--------|-------------|----------------|------|
| KPI Value | Inter | 32px | 700 | 1.2 | -0.02em | 数値表示。`font-feature-settings: "tnum"` |
| Page Title | Noto Sans JP | 15px | 600 | 1.4 | -0.01em | ヘッダー h1 |
| Section Title | Noto Sans JP | 14px | 600 | 1.4 | 0.02em | カード・セクション見出し |
| Body | Noto Sans JP | 14px | 400 | 1.8 | 0.04em | 本文テキスト |
| Label | Noto Sans JP | 13px | 500 | 1.6 | 0.04em | フォームラベル、KPIラベル |
| Table Header | Noto Sans JP | 12px | 600 | 1.5 | 0.05em | `text-transform: uppercase` は削除（日本語に不適） |
| Caption | Noto Sans JP | 11px | 400 | 1.6 | 0.04em | KPIサブテキスト、注釈 |
| Trade Tag | Noto Sans JP | 10px | 600 | 1.4 | 0 | 業種タグ |

### 3.5 行間・字間

- **本文の行間 (line-height)**: `1.8`（日本語の読みやすさを優先。旧値 1.5 から引き上げ）
- **見出しの行間**: `1.4`
- **KPI数値の行間**: `1.2`（数値はタイトでOK）
- **本文の字間 (letter-spacing)**: `0.04em`（全角文字の可読性向上）
- **見出しの字間**: `0.02em`
- **テーブルヘッダーの字間**: `0.05em`
- **KPI数値の字間**: `-0.02em`（数値はタイトに詰める）

**ガイドライン**:
- 日本語本文は `line-height: 1.8` を基準とする（1.5 では行が詰まって読みにくい）
- `letter-spacing: 0.04em` は全角文字の基本。見出しはやや狭く、テーブルヘッダーはやや広く
- KPI等の数値表示は欧文ルール（タイトな行間・字間）に従う
- `text-transform: uppercase` は日本語テーブルヘッダーでは使用しない

### 3.6 禁則処理・改行ルール

```css
/* 全体に適用 */
word-break: break-all;
overflow-wrap: break-word;
line-break: strict;
```

**禁則対象**:
- 行頭禁止: `）」』】〕〉》」】、。，．・：；？！`
- 行末禁止: `（「『【〔〈《「【`
- 会社名・許可番号は途中で折り返さないよう `white-space: nowrap` を個別指定

### 3.7 OpenType 機能

```css
/* 見出し・ナビゲーション — プロポーショナル字詰め */
font-feature-settings: "palt" 1, "kern" 1;

/* KPI数値 — テーブル数字 */
font-feature-settings: "tnum" 1;

/* 本文 — palt は適用しない（可読性優先） */
font-feature-settings: "kern" 1;
```

- **palt**: 見出し・ナビボタン等の短いテキストに適用。本文には非適用（可読性低下のため）
- **kern**: 和欧混植時のカーニング。全体に適用
- **tnum**: KPI数値のテーブル数字。桁揃えに有効

### 3.8 縦書き

- 該当なし。本システムは横書きのみ

---

## 4. Component Stylings

### Buttons

**Primary（CTA）**
- Background: `var(--primary)` (`#0075de`)
- Text: `#ffffff`
- Padding: 6px 14px
- Border Radius: 4px
- Font Size: 14px
- Font Weight: 500
- Hover: `opacity: 0.85`
- Transition: `opacity 0.15s`

**Secondary（アクションボタン）**
- Background: `#ffffff`
- Text: `var(--text)`
- Border: 1px solid `var(--border)`
- Padding: 6px 14px
- Border Radius: 4px
- Font Weight: 500
- Hover: `background: rgba(55,53,47,0.03)`

**Danger**
- Border: 1px solid `var(--danger)`
- Text: `var(--danger)`

**Success**
- Border: 1px solid `var(--ok)`
- Text: `var(--ok)`

### Inputs

- Background: `var(--card)` (`#ffffff`)
- Border: 1px solid `rgba(55,53,47,0.16)`
- Border (focus): 1px solid `var(--primary)`
- Focus Shadow: `0 0 0 2px rgba(0,117,222,0.15)`
- Border Radius: 4px
- Padding: 8px 12px
- Font Size: 14px
- Font Family: `inherit`（和文フォントチェーンを継承）

### Cards（KPI / Chart / Detail Section）

- Background: `var(--card)` (`#ffffff`)
- Border: 1px solid `var(--border)`
- Border Radius: 8px
- Padding: 16px 20px（KPI）/ 20px（チャート・詳細）
- Shadow: なし（フラットデザイン）
- KPIカードのみ: 左ボーダー 4px solid（ステータス色）

### Status Chips

- Padding: 2px 8px
- Border Radius: 3px
- Font Size: 12px
- Font Weight: 500
- バリエーション: `chip-danger` / `chip-warn` / `chip-ok` / `chip-unknown`

### Tables

- Border Collapse: `separate`（`border-spacing: 0`）
- Border: 1px solid `var(--border)`
- Border Radius: 8px（角丸テーブル）
- Header BG: `#f7f6f3`
- Header Font: 12px / 600 / `letter-spacing: 0.05em`
- Cell Padding: 8px 14px
- Row Hover: `rgba(55,53,47,0.02)`
- Row Border: `rgba(55,53,47,0.06)`

---

## 5. Layout Principles

### Spacing Scale

| Token | Value | 用途 |
|-------|-------|------|
| XS | 2px | ナビボタン間のギャップ |
| S | 4px | KPIサブテキストのマージン |
| M | 8px | ツールバーギャップ、Andonマージン |
| L | 12px | KPIグリッドギャップ、チャートギャップ |
| XL | 16px | カード内パディング、詳細セクション余白 |
| XXL | 20px | KPIカードパディング、ページ上部余白 |
| XXXL | 24px | ページ左右パディング、詳細パネル全体余白 |

### Container

- Max Width: 制約なし（GAS サイドバー / 全画面）
- Horizontal Padding: 24px

### Grid

- KPI行: `grid-template-columns: repeat(4, 1fr)` / gap: 12px
- チャート行: `grid-template-columns: 2fr 1fr` / gap: 12px

---

## 6. Depth & Elevation

| Level | Shadow | 用途 |
|-------|--------|------|
| 0 | none | カード、テーブル、ボタン（デフォルト） |
| 1 | `0 2px 10px rgba(0,0,0,0.12)` | トースト通知のみ |

- **設計方針**: Notion同様、シャドウを極力排除。ボーダーと背景色の透過度で階層を表現
- トースト以外でシャドウを使わない

---

## 7. Do's and Don'ts

### Do（推奨）

- フォントは `Inter` → `Noto Sans JP` → システムフォント → `sans-serif` の順で指定する
- 日本語本文の line-height は `1.8` にする（業務UIの可読性確保）
- 日本語本文の letter-spacing は `0.04em` にする
- 見出し・ナビに `font-feature-settings: "palt" 1` を適用する
- 色のコントラスト比は WCAG AA 以上を確保する
- ステータス表現は左ボーダー色 + 薄い背景色のペアで行う
- 会社名・許可番号は `white-space: nowrap` で折り返しを防ぐ
- テーブルヘッダーは日本語ラベル（漢字）を使う

### Don't（禁止）

- `font-family` に `Inter` だけを指定しない（日本語が豆腐になる）
- 日本語本文に `line-height: 1.5` 以下を使わない（欧文基準では日本語が詰まる）
- テーブルヘッダーに `text-transform: uppercase` を使わない（日本語に無意味）
- テキストの色に純粋な `#000000` を使わない。`rgba(55,53,47,0.95)` を使用
- `box-shadow` でカードの階層を表現しない。ボーダーと背景透過度で表現する
- 本文に `font-feature-settings: "palt"` を適用しない（長文の可読性が低下する）
- 全角・半角スペースを混在させない

---

## 8. Responsive Behavior

### Breakpoints

| Name | Width | 説明 |
|------|-------|------|
| Mobile | ≤ 768px | KPI 2列、チャート 1列、ツールバー縦積み |
| Desktop | > 768px | KPI 4列、チャート 2:1、ツールバー横並び |

### モバイル対応

```css
@media (max-width: 768px) {
  .kpi-row { grid-template-columns: repeat(2, 1fr); }
  .chart-row { grid-template-columns: 1fr; }
  .toolbar { flex-direction: column; }
  .toolbar input { width: 100%; }
  .header { flex-direction: column; gap: 8px; }
  .nav-pills { flex-wrap: wrap; }
}
```

### タッチターゲット

- 最小サイズ: 44px × 44px（WCAG基準）
- ナビボタン・ツールバーボタンは padding で確保

### フォントサイズの調整

- モバイルでも本文 14px を維持（最小サイズ）
- KPI数値は 28px に縮小可

---

## 9. Agent Prompt Guide

### クイックリファレンス

```
Primary Color: #0075de
Danger: #e03e3e
Warning: #d9730d
Success: #0f7b6c

Text Primary: rgba(55,53,47,0.95)
Text Secondary: rgba(55,53,47,0.65)
Text Muted: rgba(55,53,47,0.45)

Background: #f7f6f3
Surface: #ffffff
Border: rgba(55,53,47,0.09)

Font: 'Inter', 'Noto Sans JP', -apple-system, BlinkMacSystemFont,
  'Segoe UI', 'Hiragino Kaku Gothic ProN', 'Yu Gothic', Meiryo, sans-serif

Body Size: 14px
Body Line Height: 1.8
Body Letter Spacing: 0.04em
Heading Line Height: 1.4
Heading Letter Spacing: 0.02em

Button Radius: 4px
Card Radius: 8px
Input Radius: 4px
```

### プロンプト例

```
建設業許可証管理システムのデザインに従って、会社一覧テーブルを作成してください。
- フォント: 'Inter', 'Noto Sans JP', sans-serif
- 本文: 14px / line-height: 1.8 / letter-spacing: 0.04em
- テキスト色: rgba(55,53,47,0.95)
- テーブルヘッダー: 背景 #f7f6f3、12px / weight 600 / letter-spacing: 0.05em
- テーブル枠: rgba(55,53,47,0.09)、角丸 8px
- ステータス: chip-danger(#e03e3e) / chip-warn(#d9730d) / chip-ok(#0f7b6c)
- ホバー: rgba(55,53,47,0.02)
- 禁則処理: line-break: strict; overflow-wrap: break-word;
```

---
