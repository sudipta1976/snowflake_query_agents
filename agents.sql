CREATE DATABASE IF NOT EXISTS QUERY_OPTIMIZATION_DB;
CREATE SCHEMA IF NOT EXISTS QUERY_OPTIMIZATION_DB.AGENT;

USE DATABASE QUERY_OPTIMIZATION_DB;
USE SCHEMA AGENT;

-- ============================================================================
-- TIER 1: RAW BAD QUERIES TABLE (populated hourly, no LLM)
-- ============================================================================

CREATE OR REPLACE TABLE QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW (
    CAPTURE_ID NUMBER AUTOINCREMENT,
    CAPTURE_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    QUERY_ID VARCHAR,
    WAREHOUSE_NAME VARCHAR,
    WAREHOUSE_SIZE VARCHAR,
    USER_NAME VARCHAR,
    ROLE_NAME VARCHAR,
    QUERY_TYPE VARCHAR,
    EXECUTION_STATUS VARCHAR,
    QUERY_DATE DATE,
    TOTAL_ELAPSED_TIME_SEC FLOAT,
    TOTAL_CREDITS FLOAT,
    TOTAL_COST_USD FLOAT,
    GB_SCANNED FLOAT,
    PERCENTAGE_SCANNED_FROM_CACHE FLOAT,
    PARTITION_SCAN_PCT FLOAT,
    SPILL_STATUS VARCHAR,
    QUEUE_STATUS VARCHAR,
    QUERY_TEXT VARCHAR(16000),
    CLIENT_APPLICATION_ID VARCHAR(200),
    CLIENT_SOURCE VARCHAR(200),
    HAS_ANTI_PATTERN BOOLEAN,
    ANTI_PATTERN_REASON VARIANT,
    IS_ANALYZED BOOLEAN DEFAULT FALSE
);

SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW;

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INTEGER;
    last_collection_time TIMESTAMP_LTZ;
BEGIN
    -- Get the timestamp of last collected query (or default to 24 hours ago if no records)
    SELECT COALESCE(MAX(CAPTURE_TIMESTAMP), DATEADD(HOUR, -24, CURRENT_TIMESTAMP()))
    INTO last_collection_time
    FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW;

    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW (
        QUERY_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, USER_NAME, ROLE_NAME,
        QUERY_TYPE, EXECUTION_STATUS, QUERY_DATE, TOTAL_ELAPSED_TIME_SEC,
        TOTAL_CREDITS, TOTAL_COST_USD, GB_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE,
        PARTITION_SCAN_PCT, SPILL_STATUS, QUEUE_STATUS, QUERY_TEXT,
        CLIENT_APPLICATION_ID, CLIENT_SOURCE, HAS_ANTI_PATTERN, ANTI_PATTERN_REASON
    )
    WITH QUERY_COST_BASE AS (
    SELECT
        qh.QUERY_ID,
        qh.WAREHOUSE_NAME,
        qh.WAREHOUSE_SIZE,
        qh.USER_NAME,
        qh.ROLE_NAME,
        qh.QUERY_TYPE,
        qh.EXECUTION_STATUS,
        DATE(qh.START_TIME) AS QUERY_DATE,
        qh.TOTAL_ELAPSED_TIME / 1000.0 AS TOTAL_ELAPSED_TIME_SEC,
        COALESCE(qa.CREDITS_ATTRIBUTED_COMPUTE, 0) + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0) AS TOTAL_CREDITS,
        ROUND((COALESCE(qa.CREDITS_ATTRIBUTED_COMPUTE, 0) + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0) + COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) * 1.83, 4) AS TOTAL_COST_USD,
        qh.BYTES_SCANNED / POWER(1024, 3) AS GB_SCANNED,
        qh.PERCENTAGE_SCANNED_FROM_CACHE,
        CASE WHEN qh.PARTITIONS_TOTAL > 0 THEN ROUND(qh.PARTITIONS_SCANNED * 100.0 / qh.PARTITIONS_TOTAL, 2) ELSE 0 END AS PARTITION_SCAN_PCT,
        CASE 
            WHEN qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 'Remote Spill'
            WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 THEN 'Local Spill'
            ELSE 'No Spill'
        END AS SPILL_STATUS,
        CASE
            WHEN qh.QUEUED_OVERLOAD_TIME > 0 THEN 'Queued'
            ELSE 'Not Queued'
        END AS QUEUE_STATUS,
        LEFT(qh.QUERY_TEXT, 16000) AS QUERY_TEXT,
        s.CLIENT_APPLICATION_ID,
        CASE 
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%Snowflake UI%' THEN 'Snowflake UI'
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%SnowSQL%' THEN 'SnowSQL'
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%Python%' THEN 'Python Connector'
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%JDBC%' THEN 'JDBC'
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%ODBC%' THEN 'ODBC'
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%Spark%' THEN 'Spark'
            WHEN s.CLIENT_APPLICATION_ID ILIKE '%dbt%' THEN 'dbt'
            ELSE s.CLIENT_APPLICATION_ID
        END AS CLIENT_SOURCE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
        ON qh.QUERY_ID = qa.QUERY_ID
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s
        ON qh.SESSION_ID = s.SESSION_ID
    WHERE qh.START_TIME > DATEADD(DAY, -7, CURRENT_TIMESTAMP())
      AND qh.QUERY_TYPE IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE_TABLE_AS_SELECT')
      AND qh.QUERY_ID NOT IN (SELECT QUERY_ID FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW)
),
FILTERED_QUERIES AS (
    SELECT *
    FROM QUERY_COST_BASE
    WHERE TOTAL_COST_USD > 0.01
       OR TOTAL_ELAPSED_TIME_SEC > 60
       OR SPILL_STATUS != 'No Spill'
       OR PARTITION_SCAN_PCT > 50
    ORDER BY TOTAL_COST_USD DESC
    LIMIT 10
),
SCORED_QUERIES AS (
    SELECT 
        *,
        AI_FILTER(
            PROMPT('You are a Snowflake query optimization expert.

Evaluate the SQL query below and determine if it contains performance anti-patterns.

Consider:
1. Data scanning inefficiency (full table scans, high partition scan %)
2. Poor filtering (missing or weak WHERE clause)
3. SELECT * usage
4. Join inefficiencies (CROSS JOIN, missing join conditions)
5. Non-sargable predicates (functions on indexed/filter columns)
6. Spilling risk patterns (large joins, aggregations without pruning)
7. Redundant or repeated subqueries
8. Lack of partition pruning

Return:
TRUE  -> if query is likely inefficient
FALSE -> if query is reasonably optimized

Be strict: only return TRUE if there is clear evidence.

Query:
{0}', QUERY_TEXT)
        ) AS HAS_ANTI_PATTERN
    FROM FILTERED_QUERIES
)
SELECT 
    *,
    PARSE_JSON(
    REPLACE(
    AI_COMPLETE(
        'llama3.1-8b',
        CONCAT('Return ONLY a JSON array Format: [{"analysis_number":1,"analysis":"desc"}] list the performance anti-patterns found in this SQL query (e.g., SELECT *, missing WHERE, CROSS JOIN, non-sargable predicates). No text before or after. SQL: ', QUERY_TEXT)
    ), '\\\"', '\"') )
    AS ANTI_PATTERN_REASON
FROM SCORED_QUERIES
WHERE HAS_ANTI_PATTERN = TRUE
ORDER BY TOTAL_COST_USD DESC;
    
    rows_inserted := SQLROWCOUNT;
    RETURN 'Collected ' || rows_inserted || ' bad queries since ' || last_collection_time::VARCHAR;
END;
$$;

-- Hourly task to collect bad queries (NO LLM cost!)
CREATE OR REPLACE TASK QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 MINUTES'
    COMMENT = 'Collects bad queries every hour (no LLM cost)'
AS
    CALL QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES();

ALTER TASK QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES_TASK RESUME;

execute task QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES_TASK;


select * from QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW;


-- Check RUN_AGENT_DAILY_TASK
SELECT * FROM TABLE(QUERY_OPTIMIZATION_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'COLLECT_BAD_QUERIES_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
)) ORDER BY SCHEDULED_TIME DESC;


-- ============================================================================
-- TIER 2: ANALYSIS RESULTS TABLE
-- ============================================================================

CREATE OR REPLACE TABLE QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS (
    ANALYSIS_ID NUMBER AUTOINCREMENT,
    ANALYSIS_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CAPTURE_ID NUMBER,
    QUERY_ID VARCHAR,
    KEY_FINDINGS VARCHAR(4000),
    OPTIMIZATION_SUGGESTIONS VARCHAR(4000),
    SUGGESTED_QUERY_REWRITE VARCHAR(16000)
);

-- ============================================================================
-- TIER 2: DAILY ANALYSIS (using CORTEX.COMPLETE)
-- ============================================================================

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.ANALYZE_AND_SAVE_QUERIES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_analyzed INTEGER := 0;
BEGIN
    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS (
        CAPTURE_ID, QUERY_ID, KEY_FINDINGS, OPTIMIZATION_SUGGESTIONS, SUGGESTED_QUERY_REWRITE
    )
    SELECT 
        b.CAPTURE_ID,
        b.QUERY_ID,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            CONCAT(
                'You are a Snowflake performance expert. Identify performance issues in 2-3 bullet points. Be concise.\n',
                '- Status: ', b.EXECUTION_STATUS, '\n',
                '- Cost: $', ROUND(b.TOTAL_COST_USD, 2)::VARCHAR, '\n',
                '- Runtime: ', ROUND(b.TOTAL_ELAPSED_TIME_SEC/60, 1)::VARCHAR, ' min\n',
                '- Data Scanned: ', ROUND(b.GB_SCANNED, 1)::VARCHAR, ' GB\n',
                '- Cache Hit: ', ROUND(b.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, '%\n',
                '- Partitions Scanned: ', ROUND(b.PARTITION_SCAN_PCT, 1)::VARCHAR, '%\n',
                '- Spill: ', b.SPILL_STATUS, '\n',
                '- Warehouse: ', COALESCE(b.WAREHOUSE_SIZE, 'Unknown'), '\n',
                'List ONLY the issues found.'
            )
        ) AS KEY_FINDINGS,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            CONCAT(
                'You are a Snowflake optimization expert. Provide 2-3 actionable fixes. Be concise.\n',
                '- Type: ', b.QUERY_TYPE, '\n',
                '- Cost: $', ROUND(b.TOTAL_COST_USD, 2)::VARCHAR, '\n',
                '- Data Scanned: ', ROUND(b.GB_SCANNED, 1)::VARCHAR, ' GB\n',
                '- Cache Hit: ', ROUND(b.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, '%\n',
                '- Partitions Scanned: ', ROUND(b.PARTITION_SCAN_PCT, 1)::VARCHAR, '%\n',
                '- Spill: ', b.SPILL_STATUS, '\n',
                '- Warehouse: ', COALESCE(b.WAREHOUSE_SIZE, 'Unknown'), '\n',
                '- Query: ', LEFT(b.QUERY_TEXT, 500), '\n',
                'Provide ONLY actionable fixes.'
            )
        ) AS OPTIMIZATION_SUGGESTIONS,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            CONCAT(
                'You are a Snowflake SQL expert. Given this problematic query, suggest an optimized rewrite.\n',
                'ORIGINAL QUERY:\n', LEFT(b.QUERY_TEXT, 1500), '\n\n',
                '- Anti-patterns: ', REPLACE(b.ANTI_PATTERN_REASON::VARCHAR, '"', ''), '\n',
                'PERFORMANCE ISSUES:\n',
                '- Data Scanned: ', ROUND(b.GB_SCANNED, 1)::VARCHAR, ' GB\n',
                '- Cache Hit: ', ROUND(b.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, '%\n',
                '- Partitions Scanned: ', ROUND(b.PARTITION_SCAN_PCT, 1)::VARCHAR, '%\n',
                '- Spill: ', b.SPILL_STATUS, '\n\n',
                'Provide ONLY the optimized SQL query. Consider:\n',
                '1. Add appropriate WHERE clauses for partition pruning\n',
                '2. Use clustering keys if applicable\n',
                '3. Limit columns in SELECT\n',
                '4. Add LIMIT if possible\n',
                '5. Optimize JOINs\n',
                'Return ONLY the optimized SQL, no explanation.'
            )
        ) AS SUGGESTED_QUERY_REWRITE
        
    FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW b
    WHERE b.IS_ANALYZED = FALSE
    ORDER BY b.TOTAL_COST_USD DESC;
    -- LIMIT 10;
    
    rows_analyzed := SQLROWCOUNT;
    
    UPDATE QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW
    SET IS_ANALYZED = TRUE
    WHERE CAPTURE_ID IN (
        SELECT CAPTURE_ID FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS
    );
    
    RETURN 'Analyzed ' || rows_analyzed || ' queries.';
END;
$$;

-- Daily task to run analysis (once per day at 6 AM)
CREATE OR REPLACE TASK QUERY_OPTIMIZATION_DB.AGENT.RUN_ANALYSIS_DAILY_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    COMMENT = 'Analyzes bad queries daily at 6 AM UTC using CORTEX.COMPLETE'
AS
    CALL QUERY_OPTIMIZATION_DB.AGENT.ANALYZE_AND_SAVE_QUERIES();

ALTER TASK QUERY_OPTIMIZATION_DB.AGENT.RUN_ANALYSIS_DAILY_TASK RESUME;
ALTER TASK QUERY_OPTIMIZATION_DB.AGENT.RUN_ANALYSIS_DAILY_TASK SUSPEND;
SHOW TASKS; QUERY_OPTIMIZATION_DB.AGENT.RUN_ANALYSIS_DAILY_TASK;


execute task QUERY_OPTIMIZATION_DB.AGENT.RUN_ANALYSIS_DAILY_TASK;


SELECT * FROM TABLE(QUERY_OPTIMIZATION_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'RUN_ANALYSIS_DAILY_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
)) ORDER BY SCHEDULED_TIME DESC;


SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS ;

select current_timestamp();

-- ============================================================================
-- ALERT: Notify when new query analyses are created (today only)
-- ============================================================================

CREATE OR REPLACE NOTIFICATION INTEGRATION QUERY_OPTIMIZATION_EMAIL_INT
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('sarkar.sudipta1976@gmail.com');

GRANT EXECUTE ALERT ON ACCOUNT TO ROLE ACCOUNTADMIN;

CREATE OR REPLACE ALERT QUERY_OPTIMIZATION_DB.AGENT.NEW_BAD_QUERIES_ALERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 7 * * * UTC'  -- Runs at 7 AM UTC (after 6 AM analysis task)
    IF (EXISTS (
        SELECT 1 
        FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS
        WHERE DATE(ANALYSIS_TIMESTAMP) = CURRENT_DATE()
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'QUERY_OPTIMIZATION_EMAIL_INT',
            'sarkar.sudipta1976@gmail.com',
            'Query Optimization Alert: New Bad Queries Found Today',
            (SELECT 
                'The Query Optimization system has analyzed bad queries today.\n\n' ||
                'Date: ' || CURRENT_DATE()::VARCHAR || '\n' ||
                'Queries Analyzed: ' || COUNT(*)::VARCHAR || '\n\n' ||
                'Top Issues Found:\n' ||
                LISTAGG(
                    '• ' || QUERY_ID || ': ' || LEFT(REPLACE(KEY_FINDINGS, '\n', ' '), 150),
                    '\n'
                ) WITHIN GROUP (ORDER BY ANALYSIS_TIMESTAMP DESC) || '\n\n' ||
                'To view full details, run:\n' ||
                'SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS WHERE DATE(ANALYSIS_TIMESTAMP) = CURRENT_DATE();'
            FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS
            WHERE DATE(ANALYSIS_TIMESTAMP) = CURRENT_DATE()
            )
        );

ALTER ALERT QUERY_OPTIMIZATION_DB.AGENT.NEW_BAD_QUERIES_ALERT RESUME;

EXECUTE ALERT QUERY_OPTIMIZATION_DB.AGENT.NEW_BAD_QUERIES_ALERT;


SHOW ALERTS LIKE 'NEW_BAD_QUERIES_ALERT';
DESCRIBE ALERT QUERY_OPTIMIZATION_DB.AGENT.NEW_BAD_QUERIES_ALERT;

-- ============================================================================
-- HELPER QUERIES
-- ============================================================================


SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS WHERE DATE(ANALYSIS_TIMESTAMP) = CURRENT_DATE();
SELECT 
    r.QUERY_ID,
    b.USER_NAME,
    b.TOTAL_COST_USD,
    b.SPILL_STATUS,
    r.KEY_FINDINGS,
    r.OPTIMIZATION_SUGGESTIONS,
    r.SUGGESTED_QUERY_REWRITE,
    r.ANALYSIS_TIMESTAMP
FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS r
JOIN QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW b ON r.CAPTURE_ID = b.CAPTURE_ID
ORDER BY r.ANALYSIS_TIMESTAMP DESC;

-- Check task status
-- SHOW TASKS IN SCHEMA QUERY_OPTIMIZATION_DB.AGENT;

-- View task history
-- SELECT * FROM TABLE(QUERY_OPTIMIZATION_DB.INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'COLLECT_BAD_QUERIES_TASK',
--     SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
-- )) ORDER BY SCHEDULED_TIME DESC;

-- Manually run collection
-- CALL QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES();

-- Manually run analysis
-- CALL QUERY_OPTIMIZATION_DB.AGENT.ANALYZE_AND_SAVE_QUERIES();



CREATE OR REPLACE INTERACTIVE TABLE QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS_IT
  CLUSTER BY (CAPTURE_ID, QUERY_ID)
  TARGET_LAG = '5 minutes'
  WAREHOUSE = INTERACTIVE_WH
AS
  SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS;

  
CREATE OR REPLACE INTERACTIVE TABLE QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW_IT
  CLUSTER BY (CAPTURE_ID, QUERY_ID)
  TARGET_LAG = '5 minutes'
  WAREHOUSE = INTERACTIVE_WH

AS
  SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW;

  select * from QUERY_ANALYSIS_RESULTS_IT;
