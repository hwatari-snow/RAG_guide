# 景品表示法 RAG Agent デモ

消費者庁が公開している景品表示法ガイドラインPDF（33件）を使い、Snowflake上でRAGパイプラインとCortex Agentを構築するデモプロジェクトです。

## アーキテクチャ

```
GitHub (PDF) → Git Integration → Snowflake Stage
                                      ↓
                              AI_PARSE_DOCUMENT
                                      ↓
                        SPLIT_TEXT_RECURSIVE_CHARACTER
                                      ↓
                            Cortex Search Service
                                      ↓
                              Cortex Agent
```

## ファイル構成

| ファイル | 内容 |
|----------|------|
| `setup.sql` | Git Integration・ステージ作成・PDF取り込み |
| `create_rag_agent.sql` | RAGパイプライン構築（パース→チャンク→Search→Agent） |
| `create_rag_agent.ipynb` | 上記SQLのNotebook版（Snowflake Workspace用） |
| `pdf/` | 消費者庁ガイドラインPDF（33件） |

## セットアップ手順

### 1. 前提条件

- Snowflakeアカウント（ACCOUNTADMIN ロール）
- GitHub リポジトリ: https://github.com/hwatari-snow/RAG_guide

### 2. 環境構築（setup.sql）

`setup.sql` を実行して以下を作成します:

- データベース `DEMO` / スキーマ `KAIBALAB`
- API Integration（GitHub連携用）
- Git Repository（`RAG_GUIDE_REPO`）
- ステージ `RAG_PDF`（暗号化: `SNOWFLAKE_SSE`）
- GitHubからPDFをステージにコピー

### 3. RAGパイプライン構築（create_rag_agent.sql / .ipynb）

| Step | 処理 | 出力オブジェクト |
|------|------|-----------------|
| 1 | `AI_PARSE_DOCUMENT` でPDFパース | `RAG_PDF_PARSED` テーブル |
| 2 | ファイル名→URL マッピング作成 | `RAG_PDF_URL_MAP` テーブル |
| 3 | `SPLIT_TEXT_RECURSIVE_CHARACTER` でチャンク分割 | `RAG_PDF_CHUNKS` テーブル |
| 4 | Cortex Search Service 作成 | `CAA_GUIDELINE_SEARCH` |
| 5 | Cortex Agent 作成 | `LEGAL_GUIDE_AGENT` |

## 使用しているSnowflake機能

- **Git Integration** — GitHubリポジトリとSnowflakeの連携
- **AI_PARSE_DOCUMENT** — PDFからテキスト抽出（LAYOUTモード）
- **SPLIT_TEXT_RECURSIVE_CHARACTER** — markdownモードでのテキストチャンク分割
- **Cortex Search Service** — ベクトル検索＋テキスト検索のハイブリッド検索
- **Cortex Agent** — LLMベースのエージェント（検索ツール付き）

## Snowflakeオブジェクト一覧

```
DEMO.KAIBALAB
├── API Integration: GIT_API_INTEGRATION_RAG_GUIDE
├── Git Repository:  RAG_GUIDE_REPO
├── Stage:           RAG_PDF (SNOWFLAKE_SSE)
├── Tables:
│   ├── RAG_PDF_PARSED
│   ├── RAG_PDF_URL_MAP
│   └── RAG_PDF_CHUNKS
├── Cortex Search:   CAA_GUIDELINE_SEARCH
└── Agent:           LEGAL_GUIDE_AGENT
```

## Agent の設定ポイント

- `id_column: source_url` — citationリンクのURLとして消費者庁の元PDFリンクを使用
- `title_column: doc_title` — citationリンクのタイトルとしてガイドライン名を使用
- `metadata_columns` — 検索結果に `doc_title`, `source_url`, `file_name` を含める

## サンプル質問

- おとり広告とは何ですか？どのような場合に該当しますか？
- 二重価格表示が不当表示となるのはどのような場合ですか？
- ステルスマーケティングの規制基準を教えてください
- コンプガチャは景品表示法でどのように規制されていますか？
- 商品の原産国表示に関するルールを教えてください
