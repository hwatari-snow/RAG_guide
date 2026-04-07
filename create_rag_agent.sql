-- =============================================================
-- 景品表示法 RAG パイプライン構築スクリプト
--
-- ステージ上のPDFを AI_PARSE_DOCUMENT でパースし、
-- チャンク分割後 Cortex Search Service で検索可能にする。
-- 新規PDFが追加されたら Stream + Task で自動取り込みする。
-- =============================================================

USE ROLE accountadmin;
USE SCHEMA DEMO.KAIBALAB;
USE WAREHOUSE COMPUTE_WH;

-- Cortex AI のクロスリージョン推論を有効化（他リージョンのモデルも利用可能にする）
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- =============================================================
-- Step 0: ステージの DIRECTORY を有効化してメタデータをリフレッシュ
--   AUTO_REFRESH=TRUE: ファイル追加時に自動でメタデータ更新（AWS限定プレビュー）
--   GCP/Azure の場合は AUTO_REFRESH=TRUE を削除し、Task 内の ALTER STAGE REFRESH で対応
-- =============================================================
ALTER STAGE DEMO.KAIBALAB.RAG_PDF SET DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);
ALTER STAGE DEMO.KAIBALAB.RAG_PDF REFRESH;

-- =============================================================
-- Step 1: AI_PARSE_DOCUMENT (OCRモード) で PDF をパースしてテーブルに格納
--   増分処理: 既にパース済みのファイルはスキップする
-- =============================================================

CREATE TABLE IF NOT EXISTS DEMO.KAIBALAB.RAG_PDF_PARSED (
    file_name VARCHAR,
    parsed_result VARIANT,
    content STRING,
    page_count INT
);

INSERT INTO DEMO.KAIBALAB.RAG_PDF_PARSED (file_name, parsed_result, content, page_count)
SELECT
    relative_path AS file_name,
    AI_PARSE_DOCUMENT(
        TO_FILE('@DEMO.KAIBALAB.RAG_PDF', relative_path),
        {'mode': 'OCR'} --LAYOUTではなくOCRへ変更
    ) AS parsed_result,
    parsed_result:content::STRING AS content,
    parsed_result:metadata:pageCount::INT AS page_count
FROM DIRECTORY(@DEMO.KAIBALAB.RAG_PDF)
WHERE relative_path ILIKE '%.pdf'
  AND relative_path NOT IN (SELECT file_name FROM DEMO.KAIBALAB.RAG_PDF_PARSED);

-- =============================================================
-- Step 2: ファイル名 → 消費者庁URL・ガイドライン名のマッピングテーブル
--   Cortex Search の検索結果に参照元URLとタイトルを付与するために使用
-- =============================================================

CREATE OR REPLACE TABLE DEMO.KAIBALAB.RAG_PDF_URL_MAP AS
SELECT column1 AS file_name, column2 AS source_url, column3 AS doc_title
FROM VALUES
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
-- Step 3: パース済みテキストを1500トークン/300オーバーラップでチャンク分割
--   URL マッピングを JOIN して各チャンクに参照元情報を付与
--   増分処理: 既にチャンク済みのファイルはスキップする
-- =============================================================

CREATE TABLE IF NOT EXISTS DEMO.KAIBALAB.RAG_PDF_CHUNKS (
    chunk_id VARCHAR,
    file_name VARCHAR,
    doc_title VARCHAR,
    source_url VARCHAR,
    chunk_index INT,
    page_count INT,
    chunk_text VARCHAR
);

INSERT INTO DEMO.KAIBALAB.RAG_PDF_CHUNKS (chunk_id, file_name, doc_title, source_url, chunk_index, page_count, chunk_text)
WITH chunked AS (
    SELECT
        p.file_name,
        p.page_count,
        c.index AS chunk_index,
        c.value::STRING AS chunk_text
    FROM DEMO.KAIBALAB.RAG_PDF_PARSED p,
        LATERAL FLATTEN(
            SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                p.content,
                'markdown',
                1500,
                300
            )
        ) c
    WHERE p.content IS NOT NULL
      AND p.file_name NOT IN (SELECT DISTINCT file_name FROM DEMO.KAIBALAB.RAG_PDF_CHUNKS)
)
SELECT
    ch.file_name || '_chunk_' || LPAD(ch.chunk_index::STRING, 4, '0') AS chunk_id,
    ch.file_name,
    COALESCE(m.doc_title, ch.file_name) AS doc_title,
    COALESCE(m.source_url, '') AS source_url,
    ch.chunk_index,
    ch.page_count,
    ch.chunk_text
FROM chunked ch
LEFT JOIN DEMO.KAIBALAB.RAG_PDF_URL_MAP m ON ch.file_name = m.file_name
ORDER BY ch.file_name, ch.chunk_index;

-- =============================================================
-- Step 4: Cortex Search Service を作成
--   chunk_text を検索対象とし、TARGET_LAG=1h でソーステーブルの変更を自動反映
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
-- Step 5: ステージの DIRECTORY テーブルに Stream を作成
--   新規ファイルの追加を検知するための変更追跡
-- =============================================================

CREATE OR REPLACE STREAM DEMO.KAIBALAB.RAG_PDF_STREAM
  ON STAGE DEMO.KAIBALAB.RAG_PDF;

-- =============================================================
-- Step 6: Serverless Task（30分ごとにStreamを確認）
--   新規PDFを検知したら: ステージリフレッシュ → パース → チャンク分割
--   Cortex Search は TARGET_LAG で自動的に再インデックスされる
-- =============================================================

CREATE OR REPLACE TASK DEMO.KAIBALAB.RAG_PDF_AUTO_INGEST
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'MEDIUM'
  SCHEDULE = 'USING CRON */30 * * * * Asia/Tokyo'
  WHEN SYSTEM$STREAM_HAS_DATA('DEMO.KAIBALAB.RAG_PDF_STREAM')
AS
BEGIN
    ALTER STAGE DEMO.KAIBALAB.RAG_PDF REFRESH;

    INSERT INTO DEMO.KAIBALAB.RAG_PDF_PARSED (file_name, parsed_result, content, page_count)
    SELECT
        relative_path AS file_name,
        AI_PARSE_DOCUMENT(
            TO_FILE('@DEMO.KAIBALAB.RAG_PDF', relative_path),
            {'mode': 'OCR'}
        ) AS parsed_result,
        parsed_result:content::STRING AS content,
        parsed_result:metadata:pageCount::INT AS page_count
    FROM DEMO.KAIBALAB.RAG_PDF_STREAM
    WHERE relative_path ILIKE '%.pdf'
      AND METADATA$ACTION = 'INSERT';

    INSERT INTO DEMO.KAIBALAB.RAG_PDF_CHUNKS (chunk_id, file_name, doc_title, source_url, chunk_index, page_count, chunk_text)
    WITH chunked AS (
        SELECT
            p.file_name,
            p.page_count,
            c.index AS chunk_index,
            c.value::STRING AS chunk_text
        FROM DEMO.KAIBALAB.RAG_PDF_PARSED p,
            LATERAL FLATTEN(
                SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                    p.content,
                    'markdown',
                    1500,
                    300
                )
            ) c
        WHERE p.content IS NOT NULL
          AND p.file_name NOT IN (SELECT DISTINCT file_name FROM DEMO.KAIBALAB.RAG_PDF_CHUNKS)
    )
    SELECT
        ch.file_name || '_chunk_' || LPAD(ch.chunk_index::STRING, 4, '0'),
        ch.file_name,
        COALESCE(m.doc_title, ch.file_name),
        COALESCE(m.source_url, ''),
        ch.chunk_index,
        ch.page_count,
        ch.chunk_text
    FROM chunked ch
    LEFT JOIN DEMO.KAIBALAB.RAG_PDF_URL_MAP m ON ch.file_name = m.file_name;
EXCEPTION
    WHEN OTHER THEN
        RAISE;
END;

ALTER TASK DEMO.KAIBALAB.RAG_PDF_AUTO_INGEST RESUME;


-- =============================================================
-- Step 7: Cortex Agent を作成
--   景品表示法の専門アシスタント。Cortex Search で関連ガイドラインを検索し回答する
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
    "orchestration": "あなたは日本の景品表示法に関する専門アシスタントです。消費者庁ガイドラインPDFに基づき日本語で回答してください。\n\n【参考資料リンクの出力ルール - 最重要】\n検索結果JSONの各レコードには \"source_url\" フィールドと \"doc_title\" フィールドがあります。\n回答末尾の参考資料では、source_urlフィールドの文字列をそのままURLとして使い、doc_titleフィールドの文字列をリンクテキストとして使ってください。\n\n出力テンプレート:\n---\n**参考資料:**\n- [{{doc_title}}]({{source_url}})\n\n実例: 検索結果に \"doc_title\":\"おとり広告に関する表示等の運用基準\", \"source_url\":\"https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_31.pdf\" がある場合:\n- [おとり広告に関する表示等の運用基準](https://www.caa.go.jp/policies/policy/representation/fair_labeling/guideline/pdf/100121premiums_31.pdf)\n\n絶対禁止: URLの推測・生成・短縮・編集・再構成。source_urlの値を一字一句そのまま使うこと。",
    "response": "日本語で簡潔かつ正確に回答し、ガイドラインの条文や基準を引用してください。\n\n回答末尾に必ず参考資料セクションを付けてください。リンクは [doc_titleの値](source_urlの値) の形式で、source_urlは検索結果のJSONから一字一句コピーしてください。URLを自分で組み立てることは禁止です。",
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
        "warehouse": "COMPUTE_WH"
      },
      "search_service": "DEMO.KAIBALAB.CAA_GUIDELINE_SEARCH",
      "search_columns": ["chunk_text"],
      "metadata_columns": ["doc_title", "source_url", "file_name"],
      "id_column": "source_url",
      "title_column": "doc_title",
      "max_results": 5
    }
  }
}
$$;
