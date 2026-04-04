USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.KAIBALAB;

USE SCHEMA DEMO.KAIBALAB;

CREATE OR REPLACE API INTEGRATION GIT_API_INTEGRATION_RAG_GUIDE
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/hwatari-snow')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY DEMO.KAIBALAB.RAG_GUIDE_REPO
  API_INTEGRATION = GIT_API_INTEGRATION_RAG_GUIDE
  ORIGIN = 'https://github.com/hwatari-snow/RAG_guide.git';

ALTER GIT REPOSITORY DEMO.KAIBALAB.RAG_GUIDE_REPO FETCH;

CREATE STAGE IF NOT EXISTS DEMO.KAIBALAB.RAG_PDF
  DIRECTORY = (ENABLE = TRUE);

COPY FILES
  INTO @DEMO.KAIBALAB.RAG_PDF
  FROM @DEMO.KAIBALAB.RAG_GUIDE_REPO/branches/main/pdf/
  PATTERN = '.*[.]pdf';

LS @DEMO.KAIBALAB.RAG_PDF;

-- =============================================================
-- Step 2: AI_PARSE_DOCUMENT で PDF をパースしてテーブルに格納
-- =============================================================

ALTER STAGE @DEMO.KAIBALAB.RAG_PDF REFRESH;

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
-- Step 3: チャンク分割テーブルを作成
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
    file_name || '_chunk_' || LPAD(chunk_index::STRING, 4, '0') AS chunk_id,
    file_name,
    chunk_index,
    page_count,
    chunk_text
FROM recursive_chunks
WHERE LENGTH(chunk_text) > 0
ORDER BY file_name, chunk_index;

-- =============================================================
-- Step 4: Cortex Search Service を作成
-- =============================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE DEMO.KAIBALAB.CAA_GUIDELINE_SEARCH
  ON chunk_text
  PRIMARY KEY (chunk_id)
  ATTRIBUTES file_name
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
AS (
    SELECT
        chunk_id,
        file_name,
        chunk_index,
        page_count,
        chunk_text
    FROM DEMO.KAIBALAB.RAG_PDF_CHUNKS
);
