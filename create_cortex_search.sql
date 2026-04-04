USE ROLE accountadmin;
USE SCHEMA DEMO.KAIBALAB;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================
-- Step 1: AI_PARSE_DOCUMENT で PDF をパースしてテーブルに格納
-- =============================================================

CREATE OR REPLACE TABLE DEMO.KAIBALAB.RAG_PDF_PARSED AS
SELECT
    relative_path AS file_name,
    AI_PARSE_DOCUMENT(
        TO_FILE('@DEMO.KAIBALAB.RAG_PDF', relative_path),
        {'mode': 'LAYOUT'}
    ) AS parsed_result,
    parsed_result:content::STRING AS content,
    parsed_result:metadata:pageCount::INT AS page_count
FROM DIRECTORY(@DEMO.KAIBALAB.RAG_PDF)
WHERE relative_path ILIKE '%.pdf';

-- =============================================================
-- Step 2: ファイル名→URL マッピングテーブルを作成
-- =============================================================

CREATE OR REPLACE TABLE DEMO.KAIBALAB.RAG_PDF_URL_MAP (
    file_name VARCHAR,
    source_url VARCHAR,
    doc_title VARCHAR
);

INSERT INTO DEMO.KAIBALAB.RAG_PDF_URL_MAP VALUES
('representation_cms216_240418_02.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/assets/representation_cms216_240418_02.pdf', '景品類等の指定の告示の運用基準'),
('100121premiums_21.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_21.pdf', '景品類の価額の算定基準'),
('100121premiums_22.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_22.pdf', '一般消費者に対する景品類の提供に関する事項の制限の運用基準'),
('120702premiums_1.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/120702premiums_1.pdf', '懸賞による景品類の提供に関する事項の制限の運用基準'),
('100121premiums_24.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_24.pdf', 'インターネット上で行われる懸賞企画の取扱い'),
('120518premiums_1.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/120518premiums_1.pdf', 'コンプガチャと景品表示法の景品規制'),
('100121premiums_25.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_25.pdf', 'くじの方法等による経済上の利益の提供（廃止）'),
('100121premiums_26.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_26.pdf', '商品の原産国に関する不当な表示の運用基準'),
('100121premiums_27.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_27.pdf', '原産国の定義に関する運用細則'),
('100121premiums_28.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_28.pdf', '衣料品の表示に関する運用細則'),
('100121premiums_29.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_29.pdf', '無果汁の清涼飲料水等についての表示の運用基準'),
('100121premiums_30.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_30.pdf', '消費者信用の融資費用に関する不当な表示の運用基準'),
('100121premiums_31.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_31.pdf', 'おとり広告に関する表示等の運用基準'),
('100121premiums_32.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_32.pdf', '不動産のおとり広告に関する表示の運用基準'),
('100121premiums_33.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_33.pdf', '有料老人ホームに関する不当な表示の運用基準'),
('representation_cms216_230328_03.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/assets/representation_cms216_230328_03.pdf', 'ステルスマーケティング規制の運用基準'),
('100121premiums_34.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_34.pdf', '不実証広告規制に関する指針'),
('100121premiums_35.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_35.pdf', '不当な価格表示についての考え方'),
('representation_cms216_201225_01.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/assets/representation_cms216_201225_01.pdf', '将来の販売価格を比較対照価格とする二重価格表示の執行方針'),
('100121premiums_36.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_36.pdf', '不当な割賦販売価格等の表示の運用基準'),
('100121premiums_37.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_37.pdf', '比較広告に関する景品表示法上の考え方'),
('100121premiums_38.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_38.pdf', '消費者向け電子商取引における表示の問題点と留意事項'),
('representation_cms216_220629_07.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/assets/representation_cms216_220629_07.pdf', 'インターネット消費者取引に係る広告表示の問題点及び留意事項'),
('140328premiums_5.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/140328premiums_5.pdf', 'メニュー・料理等の食品表示に係る考え方'),
('consumption_tax_180518_0001.pdf', 'https://www.caa.go.jp/policies/policy/representation/consumption_tax/pdf/consumption_tax_180518_0001.pdf', '消費税の軽減税率制度の実施に伴う価格表示'),
('consumption_tax_180518_0002.pdf', 'https://www.caa.go.jp/policies/policy/representation/consumption_tax/pdf/consumption_tax_180518_0002.pdf', '消費税の軽減税率制度の実施に伴う価格表示【別紙1】'),
('consumption_tax_180518_0003.pdf', 'https://www.caa.go.jp/policies/policy/representation/consumption_tax/pdf/consumption_tax_180518_0003.pdf', '消費税の軽減税率制度の実施に伴う価格表示【別紙2】'),
('fair_labeling_171225_0001.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/pdf/fair_labeling_171225_0001.pdf', '時間貸し駐車場の料金表示'),
('fair_labeling_181113_0001.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/pdf/fair_labeling_181113_0001.pdf', '携帯電話等の端末の販売に関する店頭広告表示の考え方'),
('information_other_2019_190625_0001.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/information_other/2019/pdf/information_other_2019_190625_0001.pdf', '携帯電話端末の店頭広告表示等の適正化'),
('representation_cms216_240418_03.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/assets/representation_cms216_240418_03.pdf', '課徴金納付命令の基本的要件に関する考え方'),
('representation_cms216_240418_04.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/assets/representation_cms216_240418_04.pdf', '確約手続に関する運用基準'),
('160225premiums_1.pdf', 'https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/160225premiums_1.pdf', '景品表示法における違反事例集');

-- =============================================================
-- Step 3: チャンク分割テーブルを作成（URL付き）
-- =============================================================

CREATE OR REPLACE TABLE DEMO.KAIBALAB.RAG_PDF_CHUNKS AS
WITH recursive_chunks AS (
    SELECT
        file_name,
        content,
        page_count,
        0 AS chunk_index,
        SUBSTRING(content, 1, 1500) AS chunk_text,
        LENGTH(content) AS total_length
    FROM DEMO.KAIBALAB.RAG_PDF_PARSED
    WHERE content IS NOT NULL

    UNION ALL

    SELECT
        file_name,
        content,
        page_count,
        chunk_index + 1,
        SUBSTRING(content, 1 + (chunk_index + 1) * 1200, 1500),
        total_length
    FROM recursive_chunks
    WHERE 1 + (chunk_index + 1) * 1200 <= total_length
)
SELECT
    c.file_name || '_chunk_' || LPAD(c.chunk_index::STRING, 4, '0') AS chunk_id,
    c.file_name,
    COALESCE(m.doc_title, c.file_name) AS doc_title,
    COALESCE(m.source_url, '') AS source_url,
    c.chunk_index,
    c.page_count,
    c.chunk_text
FROM recursive_chunks c
LEFT JOIN DEMO.KAIBALAB.RAG_PDF_URL_MAP m ON c.file_name = m.file_name
WHERE LENGTH(c.chunk_text) > 0
ORDER BY c.file_name, c.chunk_index;

-- =============================================================
-- Step 4: Cortex Search Service を作成
-- =============================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE DEMO.KAIBALAB.CAA_GUIDELINE_SEARCH
  ON chunk_text
  PRIMARY KEY (chunk_id)
  ATTRIBUTES file_name, doc_title, source_url
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
AS (
    SELECT
        chunk_id,
        file_name,
        doc_title,
        source_url,
        chunk_index,
        page_count,
        chunk_text
    FROM DEMO.KAIBALAB.RAG_PDF_CHUNKS
);

-- =============================================================
-- Step 5: Cortex Agent (LEGAL_GUIDE_AGENT) を作成
-- =============================================================

CREATE OR REPLACE AGENT DEMO.KAIBALAB.LEGAL_GUIDE_AGENT
  FROM SPECIFICATION $$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 900,
      "tokens": 400000
    }
  },
  "instructions": {
    "orchestration": "あなたは日本の景品表示法（不当景品類及び不当表示防止法）に関する専門的なガイドラインアシスタントです。消費者庁が公開しているガイドラインPDFの内容に基づいて、ユーザーの質問に正確に回答してください。回答は日本語で行ってください。\n\n【重要：参考資料URLについて】\n検索結果に含まれるsource_url列の値は消費者庁の正式なPDFリンクです。参考資料のURLは絶対に自分で生成・推測せず、検索結果のsource_url列の値をそのままコピーして使用してください。URLを短縮したり、パスを変更したりしないでください。\n\n【回答フォーマット】\n1. 質問に対する回答を記載\n2. 回答の末尾に必ず「参考資料」セクションを設け、検索結果のdoc_titleとsource_urlをそのまま使ってマークダウンリンク形式で記載してください。\n   例: - [比較広告に関する景品表示法上の考え方](https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_37.pdf)",
    "response": "回答は日本語で、簡潔かつ正確に行ってください。該当するガイドラインの条文や基準を引用してください。\n\n【絶対厳守】参考資料のURLは検索結果のsource_url列の値を一字一句そのまま使ってください。自分でURLを組み立てないでください。\n\n回答の最後には必ず以下の形式で参考資料を記載してください：\n\n---\n**参考資料:**\n- [検索結果のdoc_titleをそのまま記載](検索結果のsource_urlをそのまま記載)\n\n不明な場合は推測せず、その旨を伝えてください。",
    "sample_questions": [
      {"question": "おとり広告とは何ですか？どのような場合に該当しますか？", "answer": "おとり広告に関するガイドラインを検索してお答えします。"},
      {"question": "二重価格表示が不当表示となるのはどのような場合ですか？", "answer": "不当な価格表示に関するガイドラインを検索してお答えします。"},
      {"question": "ステルスマーケティングの規制基準を教えてください", "answer": "ステルスマーケティング規制の運用基準を検索してお答えします。"},
      {"question": "コンプガチャは景品表示法でどのように規制されていますか？", "answer": "コンプガチャに関するガイドラインを検索してお答えします。"},
      {"question": "商品の原産国表示に関するルールを教えてください", "answer": "原産国表示の運用基準を検索してお答えします。"}
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "search_caa_guidelines",
        "description": "消費者庁の景品表示法関係ガイドラインPDFを検索します。検索結果にはdoc_title（ガイドライン名）とsource_url（消費者庁の元PDFのURL）が含まれます。景品規制、表示規制、原産国表示、おとり広告、二重価格表示、比較広告、課徴金、確約手続、ステルスマーケティング、不実証広告、コンプガチャ等に関する質問に使用してください。"
      }
    }
  ],
  "tool_resources": {
    "search_caa_guidelines": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": ""
      },
      "search_service": "DEMO.KAIBALAB.CAA_GUIDELINE_SEARCH",
      "search_columns": ["chunk_text"],
      "metadata_columns": ["doc_title", "source_url", "file_name"],
      "max_results": 5
    }
  }
}
$$;