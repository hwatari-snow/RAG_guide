USE ROLE accountadmin;
USE SCHEMA DEMO.KAIBALAB;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================
-- RAG パイプライン自動メンテナンス
--
-- 構成:
--   Task (1時間ごと)
--     → RAG_PDF_PARSED テーブルを差分更新（新規PDFのみパース）
--     → RAG_PDF_URL_MAP に未登録のファイルを追加（URLは手動更新が必要）
--   Dynamic Table
--     → RAG_PDF_CHUNKS が RAG_PDF_PARSED の変更を自動検知して更新
--   Cortex Search Service (TARGET_LAG = 1 hour)
--     → RAG_PDF_CHUNKS の変更を自動検知して検索インデックスを更新
--
--   PDF追加 → Task実行 → PARSED更新 → CHUNKS自動更新 → Search自動更新
-- =============================================================

-- =============================================================
-- Step 1: RAG_PDF_PARSED を差分更新するストアドプロシージャ
--         既にパース済みのファイルはスキップし、新規PDFのみ追加
-- =============================================================

CREATE OR REPLACE PROCEDURE DEMO.KAIBALAB.REFRESH_RAG_PDF_PARSED()
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO DEMO.KAIBALAB.RAG_PDF_PARSED (file_name, parsed_result, content, page_count)
    SELECT
        relative_path AS file_name,
        AI_PARSE_DOCUMENT(
            TO_FILE('@DEMO.KAIBALAB.RAG_PDF', relative_path),
            {'mode': 'LAYOUT'}
        ) AS parsed_result,
        parsed_result:content::STRING AS content,
        parsed_result:metadata:pageCount::INT AS page_count
    FROM DIRECTORY(@DEMO.KAIBALAB.RAG_PDF)
    WHERE relative_path ILIKE '%.pdf'
      AND relative_path NOT IN (SELECT file_name FROM DEMO.KAIBALAB.RAG_PDF_PARSED);

    RETURN 'RAG_PDF_PARSED refreshed: new PDFs parsed';
END;
$$;

-- =============================================================
-- Step 2: 未登録ファイルを RAG_PDF_URL_MAP に追加するプロシージャ
--         source_url と doc_title は空で追加（手動で更新が必要）
-- =============================================================

CREATE OR REPLACE PROCEDURE DEMO.KAIBALAB.REFRESH_RAG_PDF_URL_MAP()
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO DEMO.KAIBALAB.RAG_PDF_URL_MAP (file_name, source_url, doc_title)
    SELECT
        p.file_name,
        '' AS source_url,
        p.file_name AS doc_title
    FROM DEMO.KAIBALAB.RAG_PDF_PARSED p
    WHERE p.file_name NOT IN (SELECT file_name FROM DEMO.KAIBALAB.RAG_PDF_URL_MAP);

    RETURN 'RAG_PDF_URL_MAP refreshed: new entries added (update source_url manually)';
END;
$$;

-- =============================================================
-- Step 3: Task を作成（1時間ごとに実行）
--         Git fetch → PDF パース → URL マップ更新
-- =============================================================

CREATE OR REPLACE TASK DEMO.KAIBALAB.REFRESH_RAG_PIPELINE
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * Asia/Tokyo'
  COMMENT = 'RAG パイプライン自動更新: Git fetch → PDF パース → URL マップ更新'
AS
BEGIN
    ALTER GIT REPOSITORY DEMO.KAIBALAB.RAG_GUIDE_REPO FETCH;
    COPY FILES
        INTO @DEMO.KAIBALAB.RAG_PDF
        FROM @DEMO.KAIBALAB.RAG_GUIDE_REPO/branches/main/pdf/
        PATTERN = '.*[.]pdf';
    CALL DEMO.KAIBALAB.REFRESH_RAG_PDF_PARSED();
    CALL DEMO.KAIBALAB.REFRESH_RAG_PDF_URL_MAP();
END;

ALTER TASK DEMO.KAIBALAB.REFRESH_RAG_PIPELINE RESUME;

-- =============================================================
-- Step 4: RAG_PDF_CHUNKS を Dynamic Table に変換
--         RAG_PDF_PARSED の変更を自動検知してチャンクを再生成
-- =============================================================

CREATE OR REPLACE DYNAMIC TABLE DEMO.KAIBALAB.RAG_PDF_CHUNKS
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
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
LEFT JOIN DEMO.KAIBALAB.RAG_PDF_URL_MAP m ON ch.file_name = m.file_name;

-- =============================================================
-- 確認用クエリ
-- =============================================================

-- Task の状態確認
SHOW TASKS IN SCHEMA DEMO.KAIBALAB;

-- Dynamic Table の状態確認
SELECT name, scheduling_state, refresh_mode
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE name = 'RAG_PDF_CHUNKS';
