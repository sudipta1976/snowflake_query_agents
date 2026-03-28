CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.ADMIN_CREATE_USER("USERNAME" VARCHAR, "PASSWORD" VARCHAR, "EMAIL" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
    create_stmt STRING;
BEGIN
    -- Construct the DDL statement
    create_stmt := ''CREATE USER '' || :username || 
                   '' PASSWORD = '''''' || :password || '''''' '' ||
                   '' EMAIL = '''''' || :email || '''''' '' ||
                   '' MUST_CHANGE_PASSWORD = TRUE'';
    
    -- Execute the command
    EXECUTE IMMEDIATE :create_stmt;
    
    RETURN ''User '' || :username || '' has been successfully created.'';
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.ADMIN_DISABLE_USER("USERNAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
    disable_stmt STRING;
BEGIN
    -- Construct the Alter statement
    disable_stmt := ''ALTER USER '' || :username || '' SET DISABLED = TRUE'';
    
    -- Execute the command
    EXECUTE IMMEDIATE :disable_stmt;
    
    RETURN ''User '' || :username || '' has been disabled.'';
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.ADMIN_ENABLE_USER("USERNAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
    enable_stmt STRING;
BEGIN
    -- Construct the Alter statement
    enable_stmt := ''ALTER USER '' || :username || '' SET DISABLED = FALSE'';
    
    -- Execute the command
    EXECUTE IMMEDIATE :enable_stmt;
    
    RETURN ''User '' || :username || '' has been disabled.'';
END';



CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.ANALYZE_AND_SAVE_QUERIES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
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
            ''mistral-large2'',
            CONCAT(
                ''You are a Snowflake performance expert. Identify performance issues in 2-3 bullet points. Be concise.\\n'',
                ''- Status: '', b.EXECUTION_STATUS, ''\\n'',
                ''- Cost: $'', ROUND(b.TOTAL_COST_USD, 2)::VARCHAR, ''\\n'',
                ''- Runtime: '', ROUND(b.TOTAL_ELAPSED_TIME_SEC/60, 1)::VARCHAR, '' min\\n'',
                ''- Data Scanned: '', ROUND(b.GB_SCANNED, 1)::VARCHAR, '' GB\\n'',
                ''- Cache Hit: '', ROUND(b.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, ''%\\n'',
                ''- Partitions Scanned: '', ROUND(b.PARTITION_SCAN_PCT, 1)::VARCHAR, ''%\\n'',
                ''- Spill: '', b.SPILL_STATUS, ''\\n'',
                ''- Warehouse: '', COALESCE(b.WAREHOUSE_SIZE, ''Unknown''), ''\\n'',
                ''List ONLY the issues found.''
            )
        ) AS KEY_FINDINGS,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''You are a Snowflake optimization expert. Provide 2-3 actionable fixes. Be concise.\\n'',
                ''- Type: '', b.QUERY_TYPE, ''\\n'',
                ''- Cost: $'', ROUND(b.TOTAL_COST_USD, 2)::VARCHAR, ''\\n'',
                ''- Data Scanned: '', ROUND(b.GB_SCANNED, 1)::VARCHAR, '' GB\\n'',
                ''- Cache Hit: '', ROUND(b.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, ''%\\n'',
                ''- Partitions Scanned: '', ROUND(b.PARTITION_SCAN_PCT, 1)::VARCHAR, ''%\\n'',
                ''- Spill: '', b.SPILL_STATUS, ''\\n'',
                ''- Warehouse: '', COALESCE(b.WAREHOUSE_SIZE, ''Unknown''), ''\\n'',
                ''- Query: '', LEFT(b.QUERY_TEXT, 500), ''\\n'',
                ''Provide ONLY actionable fixes.''
            )
        ) AS OPTIMIZATION_SUGGESTIONS,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''You are a Snowflake SQL expert. Given this problematic query, suggest an optimized rewrite.\\n'',
                ''ORIGINAL QUERY:\\n'', LEFT(b.QUERY_TEXT, 1500), ''\\n\\n'',
                ''- Anti-patterns: '', REPLACE(b.ANTI_PATTERN_REASON::VARCHAR, ''"'', ''''), ''\\n'',
                ''PERFORMANCE ISSUES:\\n'',
                ''- Data Scanned: '', ROUND(b.GB_SCANNED, 1)::VARCHAR, '' GB\\n'',
                ''- Cache Hit: '', ROUND(b.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, ''%\\n'',
                ''- Partitions Scanned: '', ROUND(b.PARTITION_SCAN_PCT, 1)::VARCHAR, ''%\\n'',
                ''- Spill: '', b.SPILL_STATUS, ''\\n\\n'',
                ''Provide ONLY the optimized SQL query. Consider:\\n'',
                ''1. Add appropriate WHERE clauses for partition pruning\\n'',
                ''2. Use clustering keys if applicable\\n'',
                ''3. Limit columns in SELECT\\n'',
                ''4. Add LIMIT if possible\\n'',
                ''5. Optimize JOINs\\n'',
                ''Return ONLY the optimized SQL, no explanation.''
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
    
    RETURN ''Analyzed '' || rows_analyzed || '' queries.'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.ANALYZE_BAD_QUERIES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    rows_inserted INTEGER;
BEGIN
    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS (
        QUERY_ID, WAREHOUSE_NAME, WAREHOUSE_SIZE, USER_NAME, ROLE_NAME,
        QUERY_TYPE, EXECUTION_STATUS, QUERY_DATE, TOTAL_ELAPSED_TIME_SEC,
        TOTAL_CREDITS, TOTAL_COST_USD, GB_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE,
        PARTITION_SCAN_PCT, SPILL_STATUS, QUEUE_STATUS, QUERY_TEXT_PREVIEW,
        KEY_FINDINGS, OPTIMIZATION_SUGGESTIONS, SUGGESTED_QUERY_REWRITE
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
                WHEN qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN ''Remote Spill''
                WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 THEN ''Local Spill''
                ELSE ''No Spill''
            END AS SPILL_STATUS,
            CASE
                WHEN qh.QUEUED_OVERLOAD_TIME > 0 THEN ''Queued''
                ELSE ''Not Queued''
            END AS QUEUE_STATUS,
            LEFT(qh.QUERY_TEXT, 2000) AS QUERY_TEXT_PREVIEW,
            qh.START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
            ON qh.QUERY_ID = qa.QUERY_ID
        WHERE qh.START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
          AND qh.QUERY_TYPE IN (''SELECT'', ''INSERT'', ''UPDATE'', ''DELETE'', ''MERGE'', ''CREATE_TABLE_AS_SELECT'')
    ),
    TOP_EXPENSIVE_QUERIES AS (
        SELECT *
        FROM QUERY_COST_BASE
        WHERE TOTAL_COST_USD > 0
           OR TOTAL_ELAPSED_TIME_SEC > 60
           OR SPILL_STATUS != ''No Spill''
           OR PARTITION_SCAN_PCT > 50
        ORDER BY TOTAL_COST_USD DESC
        LIMIT 10
    )
    SELECT 
        t.QUERY_ID,
        t.WAREHOUSE_NAME,
        t.WAREHOUSE_SIZE,
        t.USER_NAME,
        t.ROLE_NAME,
        t.QUERY_TYPE,
        t.EXECUTION_STATUS,
        t.QUERY_DATE,
        t.TOTAL_ELAPSED_TIME_SEC,
        t.TOTAL_CREDITS,
        t.TOTAL_COST_USD,
        t.GB_SCANNED,
        t.PERCENTAGE_SCANNED_FROM_CACHE,
        t.PARTITION_SCAN_PCT,
        t.SPILL_STATUS,
        t.QUEUE_STATUS,
        t.QUERY_TEXT_PREVIEW,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''You are a Snowflake performance expert. Identify performance issues in 2-3 bullet points. Be concise.\\n'',
                ''- Status: '', t.EXECUTION_STATUS, ''\\n'',
                ''- Cost: $'', ROUND(t.TOTAL_COST_USD, 2)::VARCHAR, ''\\n'',
                ''- Runtime: '', ROUND(t.TOTAL_ELAPSED_TIME_SEC/60, 1)::VARCHAR, '' min\\n'',
                ''- Data Scanned: '', ROUND(t.GB_SCANNED, 1)::VARCHAR, '' GB\\n'',
                ''- Cache Hit: '', ROUND(t.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, ''%\\n'',
                ''- Partitions Scanned: '', ROUND(t.PARTITION_SCAN_PCT, 1)::VARCHAR, ''%\\n'',
                ''- Spill: '', t.SPILL_STATUS, ''\\n'',
                ''- Warehouse: '', COALESCE(t.WAREHOUSE_SIZE, ''Unknown''), ''\\n'',
                ''List ONLY the issues found.''
            )
        ) AS KEY_FINDINGS,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''You are a Snowflake optimization expert. Provide 2-3 actionable fixes. Be concise.\\n'',
                ''- Type: '', t.QUERY_TYPE, ''\\n'',
                ''- Cost: $'', ROUND(t.TOTAL_COST_USD, 2)::VARCHAR, ''\\n'',
                ''- Data Scanned: '', ROUND(t.GB_SCANNED, 1)::VARCHAR, '' GB\\n'',
                ''- Cache Hit: '', ROUND(t.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, ''%\\n'',
                ''- Partitions Scanned: '', ROUND(t.PARTITION_SCAN_PCT, 1)::VARCHAR, ''%\\n'',
                ''- Spill: '', t.SPILL_STATUS, ''\\n'',
                ''- Warehouse: '', COALESCE(t.WAREHOUSE_SIZE, ''Unknown''), ''\\n'',
                ''- Query: '', LEFT(t.QUERY_TEXT_PREVIEW, 500), ''\\n'',
                ''Provide ONLY actionable fixes.''
            )
        ) AS OPTIMIZATION_SUGGESTIONS,
        
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''You are a Snowflake SQL expert. Given this problematic query, suggest an optimized rewrite.\\n'',
                ''ORIGINAL QUERY:\\n'', LEFT(t.QUERY_TEXT_PREVIEW, 1500), ''\\n\\n'',
                ''PERFORMANCE ISSUES:\\n'',
                ''- Data Scanned: '', ROUND(t.GB_SCANNED, 1)::VARCHAR, '' GB\\n'',
                ''- Cache Hit: '', ROUND(t.PERCENTAGE_SCANNED_FROM_CACHE, 1)::VARCHAR, ''%\\n'',
                ''- Partitions Scanned: '', ROUND(t.PARTITION_SCAN_PCT, 1)::VARCHAR, ''%\\n'',
                ''- Spill: '', t.SPILL_STATUS, ''\\n\\n'',
                ''Provide ONLY the optimized SQL query. Consider:\\n'',
                ''1. Add appropriate WHERE clauses for partition pruning\\n'',
                ''2. Use clustering keys if applicable\\n'',
                ''3. Limit columns in SELECT\\n'',
                ''4. Add LIMIT if possible\\n'',
                ''5. Optimize JOINs\\n'',
                ''Return ONLY the optimized SQL, no explanation.''
            )
        ) AS SUGGESTED_QUERY_REWRITE
    FROM TOP_EXPENSIVE_QUERIES t;
    
    rows_inserted := SQLROWCOUNT;
    RETURN ''Analysis complete. '' || rows_inserted || '' queries analyzed.'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.COLLECT_BAD_QUERIES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
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
            WHEN qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN ''Remote Spill''
            WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 THEN ''Local Spill''
            ELSE ''No Spill''
        END AS SPILL_STATUS,
        CASE
            WHEN qh.QUEUED_OVERLOAD_TIME > 0 THEN ''Queued''
            ELSE ''Not Queued''
        END AS QUEUE_STATUS,
        LEFT(qh.QUERY_TEXT, 16000) AS QUERY_TEXT,
        s.CLIENT_APPLICATION_ID,
        CASE 
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%Snowflake UI%'' THEN ''Snowflake UI''
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%SnowSQL%'' THEN ''SnowSQL''
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%Python%'' THEN ''Python Connector''
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%JDBC%'' THEN ''JDBC''
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%ODBC%'' THEN ''ODBC''
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%Spark%'' THEN ''Spark''
            WHEN s.CLIENT_APPLICATION_ID ILIKE ''%dbt%'' THEN ''dbt''
            ELSE s.CLIENT_APPLICATION_ID
        END AS CLIENT_SOURCE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
        ON qh.QUERY_ID = qa.QUERY_ID
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s
        ON qh.SESSION_ID = s.SESSION_ID
    WHERE qh.START_TIME > DATEADD(DAY, -7, CURRENT_TIMESTAMP())
      AND qh.QUERY_TYPE IN (''SELECT'', ''INSERT'', ''UPDATE'', ''DELETE'', ''MERGE'', ''CREATE_TABLE_AS_SELECT'')
      AND qh.QUERY_ID NOT IN (SELECT QUERY_ID FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW)
),
FILTERED_QUERIES AS (
    SELECT *
    FROM QUERY_COST_BASE
    WHERE TOTAL_COST_USD > 0.01
       OR TOTAL_ELAPSED_TIME_SEC > 60
       OR SPILL_STATUS != ''No Spill''
       OR PARTITION_SCAN_PCT > 50
    ORDER BY TOTAL_COST_USD DESC
    LIMIT 10
),
SCORED_QUERIES AS (
    SELECT 
        *,
        AI_FILTER(
            PROMPT(''You are a Snowflake query optimization expert.

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
{0}'', QUERY_TEXT)
        ) AS HAS_ANTI_PATTERN
    FROM FILTERED_QUERIES
)
SELECT 
    *,
    PARSE_JSON(
    REPLACE(
    AI_COMPLETE(
        ''llama3.1-8b'',
        CONCAT(''Return ONLY a JSON array Format: [{"analysis_number":1,"analysis":"desc"}] list the performance anti-patterns found in this SQL query (e.g., SELECT *, missing WHERE, CROSS JOIN, non-sargable predicates). No text before or after. SQL: '', QUERY_TEXT)
    ), ''\\\\\\"'', ''\\"'') )
    AS ANTI_PATTERN_REASON
FROM SCORED_QUERIES
WHERE HAS_ANTI_PATTERN = TRUE
ORDER BY TOTAL_COST_USD DESC;
    
    rows_inserted := SQLROWCOUNT;
    RETURN ''Collected '' || rows_inserted || '' bad queries since '' || last_collection_time::VARCHAR;
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.COLLECT_DAILY_COST_TRENDS("LOOKBACK_DAYS" NUMBER(38,0) DEFAULT 30)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    MERGE INTO QUERY_OPTIMIZATION_DB.AGENT.DAILY_COST_TRENDS AS target
    USING (
        WITH daily_stats AS (
            SELECT 
                DATE(qh.START_TIME) AS trend_date,
                qh.WAREHOUSE_NAME,
                COUNT(*) AS total_queries,
                SUM(COALESCE(qa.CREDITS_ATTRIBUTED_COMPUTE, 0) + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0)) AS total_credits,
                ROUND(SUM((COALESCE(qa.CREDITS_ATTRIBUTED_COMPUTE, 0) + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0) + COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) * 1.83), 2) AS total_cost_usd,
                ROUND(AVG(qh.TOTAL_ELAPSED_TIME / 1000.0), 2) AS avg_elapsed_time_sec,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qh.TOTAL_ELAPSED_TIME / 1000.0), 2) AS p95_elapsed_time_sec,
                ROUND(SUM(qh.BYTES_SCANNED) / POWER(1024, 3), 2) AS total_gb_scanned,
                SUM(CASE WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 1 ELSE 0 END) AS spill_query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
                ON qh.QUERY_ID = qa.QUERY_ID
            WHERE qh.START_TIME >= DATEADD(day, -:lookback_days, CURRENT_DATE())
              AND qh.WAREHOUSE_NAME IS NOT NULL
              AND qh.QUERY_TYPE IN (''SELECT'', ''INSERT'', ''UPDATE'', ''DELETE'', ''MERGE'', ''CREATE_TABLE_AS_SELECT'')
            GROUP BY 1, 2
        ),
        with_moving_avg AS (
            SELECT 
                ds.*,
                ROUND(ds.total_cost_usd / NULLIF(ds.total_queries, 0), 4) AS avg_cost_per_query,
                ROUND(ds.spill_query_count * 100.0 / NULLIF(ds.total_queries, 0), 2) AS spill_query_pct,
                ROUND(AVG(ds.total_cost_usd) OVER (
                    PARTITION BY ds.warehouse_name 
                    ORDER BY ds.trend_date 
                    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ), 2) AS cost_7d_avg
            FROM daily_stats ds
        )
        SELECT 
            *,
            ROUND((total_cost_usd - cost_7d_avg) * 100.0 / NULLIF(cost_7d_avg, 0), 2) AS cost_trend_vs_7d_avg
        FROM with_moving_avg
    ) AS source
    ON target.TREND_DATE = source.trend_date 
       AND target.WAREHOUSE_NAME = source.warehouse_name
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_QUERIES = source.total_queries,
        target.TOTAL_CREDITS = source.total_credits,
        target.TOTAL_COST_USD = source.total_cost_usd,
        target.AVG_COST_PER_QUERY = source.avg_cost_per_query,
        target.AVG_ELAPSED_TIME_SEC = source.avg_elapsed_time_sec,
        target.P95_ELAPSED_TIME_SEC = source.p95_elapsed_time_sec,
        target.TOTAL_GB_SCANNED = source.total_gb_scanned,
        target.SPILL_QUERY_COUNT = source.spill_query_count,
        target.SPILL_QUERY_PCT = source.spill_query_pct,
        target.COST_7D_AVG = source.cost_7d_avg,
        target.COST_TREND_VS_7D_AVG = source.cost_trend_vs_7d_avg,
        target.CAPTURED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        TREND_DATE, WAREHOUSE_NAME, TOTAL_QUERIES, TOTAL_CREDITS, TOTAL_COST_USD,
        AVG_COST_PER_QUERY, AVG_ELAPSED_TIME_SEC, P95_ELAPSED_TIME_SEC, TOTAL_GB_SCANNED,
        SPILL_QUERY_COUNT, SPILL_QUERY_PCT, COST_7D_AVG, COST_TREND_VS_7D_AVG
    ) VALUES (
        source.trend_date, source.warehouse_name, source.total_queries, source.total_credits, source.total_cost_usd,
        source.avg_cost_per_query, source.avg_elapsed_time_sec, source.p95_elapsed_time_sec, source.total_gb_scanned,
        source.spill_query_count, source.spill_query_pct, source.cost_7d_avg, source.cost_trend_vs_7d_avg
    );
    
    RETURN ''Daily cost trends collected successfully'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.DETECT_PEAK_PATTERNS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.PEAK_HOUR_PATTERNS (
        ANALYSIS_DATE, WAREHOUSE_NAME, PEAK_HOURS, OFF_PEAK_HOURS, 
        BUSIEST_DAY, QUIETEST_DAY, WEEKLY_COST_DISTRIBUTION
    )
    WITH hourly_data AS (
        SELECT 
            qh.WAREHOUSE_NAME,
            HOUR(qh.START_TIME) AS hour_of_day,
            DAYNAME(qh.START_TIME) AS day_name,
            COUNT(*) AS query_count,
            SUM((COALESCE(qa.CREDITS_ATTRIBUTED_COMPUTE, 0) + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0) + COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) * 1.83) AS total_cost_usd
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
            ON qh.QUERY_ID = qa.QUERY_ID
        WHERE qh.START_TIME >= DATEADD(day, -14, CURRENT_DATE())
          AND qh.WAREHOUSE_NAME IS NOT NULL
        GROUP BY 1, 2, 3
    ),
    hourly_avg AS (
        SELECT 
            WAREHOUSE_NAME,
            hour_of_day,
            AVG(query_count) AS avg_query_count
        FROM hourly_data
        GROUP BY 1, 2
    ),
    hourly_ranked AS (
        SELECT 
            WAREHOUSE_NAME,
            hour_of_day,
            avg_query_count,
            PERCENT_RANK() OVER (PARTITION BY WAREHOUSE_NAME ORDER BY avg_query_count) AS pct_rank
        FROM hourly_avg
    ),
    peak_hours AS (
        SELECT 
            WAREHOUSE_NAME,
            ARRAY_AGG(hour_of_day) WITHIN GROUP (ORDER BY hour_of_day) AS peak_hours
        FROM hourly_ranked
        WHERE pct_rank >= 0.75
        GROUP BY 1
    ),
    off_peak_hours AS (
        SELECT 
            WAREHOUSE_NAME,
            ARRAY_AGG(hour_of_day) WITHIN GROUP (ORDER BY hour_of_day) AS off_peak_hours
        FROM hourly_ranked
        WHERE pct_rank <= 0.25
        GROUP BY 1
    ),
    daily_totals AS (
        SELECT 
            WAREHOUSE_NAME,
            day_name,
            SUM(total_cost_usd) AS day_total_cost
        FROM hourly_data
        GROUP BY 1, 2
    ),
    daily_ranked AS (
        SELECT 
            WAREHOUSE_NAME,
            day_name,
            day_total_cost,
            ROW_NUMBER() OVER (PARTITION BY WAREHOUSE_NAME ORDER BY day_total_cost DESC) AS busy_rank,
            ROW_NUMBER() OVER (PARTITION BY WAREHOUSE_NAME ORDER BY day_total_cost ASC) AS quiet_rank
        FROM daily_totals
    ),
    busiest_days AS (
        SELECT WAREHOUSE_NAME, day_name AS busiest_day
        FROM daily_ranked
        WHERE busy_rank = 1
    ),
    quietest_days AS (
        SELECT WAREHOUSE_NAME, day_name AS quietest_day
        FROM daily_ranked
        WHERE quiet_rank = 1
    ),
    weekly_dist AS (
        SELECT 
            WAREHOUSE_NAME,
            OBJECT_CONSTRUCT(
                ''Monday'', SUM(CASE WHEN day_name = ''Mon'' THEN total_cost_usd ELSE 0 END),
                ''Tuesday'', SUM(CASE WHEN day_name = ''Tue'' THEN total_cost_usd ELSE 0 END),
                ''Wednesday'', SUM(CASE WHEN day_name = ''Wed'' THEN total_cost_usd ELSE 0 END),
                ''Thursday'', SUM(CASE WHEN day_name = ''Thu'' THEN total_cost_usd ELSE 0 END),
                ''Friday'', SUM(CASE WHEN day_name = ''Fri'' THEN total_cost_usd ELSE 0 END),
                ''Saturday'', SUM(CASE WHEN day_name = ''Sat'' THEN total_cost_usd ELSE 0 END),
                ''Sunday'', SUM(CASE WHEN day_name = ''Sun'' THEN total_cost_usd ELSE 0 END)
            ) AS weekly_cost_distribution
        FROM hourly_data
        GROUP BY 1
    )
    SELECT 
        CURRENT_DATE() AS analysis_date,
        p.WAREHOUSE_NAME,
        p.peak_hours,
        o.off_peak_hours,
        b.busiest_day,
        q.quietest_day,
        w.weekly_cost_distribution
    FROM peak_hours p
    JOIN off_peak_hours o ON p.WAREHOUSE_NAME = o.WAREHOUSE_NAME
    JOIN weekly_dist w ON p.WAREHOUSE_NAME = w.WAREHOUSE_NAME
    JOIN busiest_days b ON p.WAREHOUSE_NAME = b.WAREHOUSE_NAME
    JOIN quietest_days q ON p.WAREHOUSE_NAME = q.WAREHOUSE_NAME;
    
    RETURN ''Peak patterns detected successfully'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.GENERATE_AI_TREND_INSIGHTS("WAREHOUSE_FILTER" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    insight_count NUMBER := 0;
BEGIN
    -- Generate Cost Trend Insights
    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.AI_TREND_INSIGHTS (
        ANALYSIS_DATE, INSIGHT_TYPE, WAREHOUSE_NAME, INSIGHT_SUMMARY, AI_RECOMMENDATIONS, METRICS_SNAPSHOT
    )
    WITH recent_trends AS (
        SELECT 
            WAREHOUSE_NAME,
            ARRAY_AGG(OBJECT_CONSTRUCT(
                ''date'', TREND_DATE,
                ''cost'', TOTAL_COST_USD,
                ''queries'', TOTAL_QUERIES,
                ''avg_cost'', AVG_COST_PER_QUERY,
                ''trend_pct'', COST_TREND_VS_7D_AVG,
                ''spill_pct'', SPILL_QUERY_PCT,
                ''p95_time'', P95_ELAPSED_TIME_SEC
            )) WITHIN GROUP (ORDER BY TREND_DATE) AS trend_data,
            AVG(COST_TREND_VS_7D_AVG) AS avg_trend,
            MAX(TOTAL_COST_USD) AS max_daily_cost,
            AVG(SPILL_QUERY_PCT) AS avg_spill_pct
        FROM QUERY_OPTIMIZATION_DB.AGENT.DAILY_COST_TRENDS
        WHERE TREND_DATE >= DATEADD(day, -14, CURRENT_DATE())
          AND (:warehouse_filter IS NULL OR WAREHOUSE_NAME = :warehouse_filter)
        GROUP BY WAREHOUSE_NAME
    )
    SELECT 
        CURRENT_DATE(),
        ''COST_TREND'',
        WAREHOUSE_NAME,
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''Analyze this Snowflake warehouse cost trend data and provide a 2-3 sentence summary of the pattern (improving, worsening, stable). '',
                ''Include the percentage change trend. Data: '', trend_data::VARCHAR
            )
        )::VARCHAR AS insight_summary,
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''Based on this Snowflake warehouse cost trend data, provide 3-5 specific actionable recommendations to optimize costs. '',
                ''Consider: query patterns, warehouse sizing, scheduling, clustering. '',
                ''Average trend vs 7-day: '', avg_trend, ''%. '',
                ''Average spill rate: '', avg_spill_pct, ''%. '',
                ''Max daily cost: $'', max_daily_cost, ''. '',
                ''Full data: '', trend_data::VARCHAR
            )
        )::VARCHAR AS ai_recommendations,
        OBJECT_CONSTRUCT(
            ''avg_trend_pct'', avg_trend,
            ''max_daily_cost'', max_daily_cost,
            ''avg_spill_pct'', avg_spill_pct,
            ''data_points'', ARRAY_SIZE(trend_data)
        ) AS metrics_snapshot
    FROM recent_trends
    WHERE avg_trend IS NOT NULL;

    insight_count := insight_count + SQLROWCOUNT;

    -- Generate Peak Hour Insights
    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.AI_TREND_INSIGHTS (
        ANALYSIS_DATE, INSIGHT_TYPE, WAREHOUSE_NAME, INSIGHT_SUMMARY, AI_RECOMMENDATIONS, METRICS_SNAPSHOT
    )
    SELECT 
        CURRENT_DATE(),
        ''PEAK_PATTERN'',
        p.WAREHOUSE_NAME,
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''Summarize this warehouse usage pattern in 2-3 sentences. '',
                ''Peak hours: '', p.PEAK_HOURS::VARCHAR, ''. '',
                ''Off-peak hours: '', p.OFF_PEAK_HOURS::VARCHAR, ''. '',
                ''Busiest day: '', p.BUSIEST_DAY, ''. '',
                ''Quietest day: '', p.QUIETEST_DAY, ''. '',
                ''Weekly distribution: '', p.WEEKLY_COST_DISTRIBUTION::VARCHAR
            )
        )::VARCHAR,
        SNOWFLAKE.CORTEX.COMPLETE(
            ''mistral-large2'',
            CONCAT(
                ''Based on these Snowflake warehouse usage patterns, provide 3-5 scheduling optimization recommendations. '',
                ''Consider: auto-suspend timing, warehouse scaling schedules, workload shifting, multi-cluster settings. '',
                ''Peak hours: '', p.PEAK_HOURS::VARCHAR, ''. '',
                ''Off-peak hours: '', p.OFF_PEAK_HOURS::VARCHAR, ''. '',
                ''Busiest day: '', p.BUSIEST_DAY, ''. '',
                ''Quietest day: '', p.QUIETEST_DAY, ''.''
            )
        )::VARCHAR,
        OBJECT_CONSTRUCT(
            ''peak_hours'', p.PEAK_HOURS,
            ''off_peak_hours'', p.OFF_PEAK_HOURS,
            ''busiest_day'', p.BUSIEST_DAY,
            ''quietest_day'', p.QUIETEST_DAY
        )
    FROM QUERY_OPTIMIZATION_DB.AGENT.PEAK_HOUR_PATTERNS p
    WHERE p.ANALYSIS_DATE = CURRENT_DATE()
      AND (:warehouse_filter IS NULL OR p.WAREHOUSE_NAME = :warehouse_filter);

    insight_count := insight_count + SQLROWCOUNT;

    RETURN ''Generated '' || insight_count || '' AI trend insights'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.GET_OPTIMIZATION_SUMMARY()
RETURNS TABLE ("METRIC_CATEGORY" VARCHAR, "METRIC_NAME" VARCHAR, "METRIC_VALUE" VARCHAR)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'BEGIN
    LET res RESULTSET := (
        WITH cost_overview AS (
            SELECT
                MAX(TREND_DATE)::VARCHAR AS latest_date,
                SUM(TOTAL_QUERIES)::VARCHAR AS total_queries,
                ROUND(SUM(TOTAL_COST_USD), 2)::VARCHAR AS total_cost_usd,
                ROUND(SUM(TOTAL_CREDITS), 4)::VARCHAR AS total_credits,
                COUNT(DISTINCT WAREHOUSE_NAME)::VARCHAR AS warehouses_tracked
            FROM QUERY_OPTIMIZATION_DB.AGENT.DAILY_COST_TRENDS
            WHERE TREND_DATE = (SELECT MAX(TREND_DATE) FROM QUERY_OPTIMIZATION_DB.AGENT.DAILY_COST_TRENDS)
        ),
        cost_7d AS (
            SELECT
                ROUND(SUM(TOTAL_COST_USD), 2)::VARCHAR AS cost_last_7d,
                SUM(TOTAL_QUERIES)::VARCHAR AS queries_last_7d,
                ROUND(AVG(AVG_ELAPSED_TIME_SEC), 2)::VARCHAR AS avg_elapsed_7d
            FROM QUERY_OPTIMIZATION_DB.AGENT.DAILY_COST_TRENDS
            WHERE TREND_DATE >= DATEADD(DAY, -7, CURRENT_DATE())
        ),
        bad_query_stats AS (
            SELECT
                COUNT(*)::VARCHAR AS total_flagged_queries,
                SUM(CASE WHEN SPILL_STATUS != ''No Spill'' THEN 1 ELSE 0 END)::VARCHAR AS spill_queries,
                ROUND(AVG(TOTAL_COST_USD), 4)::VARCHAR AS avg_cost_per_bad_query,
                ROUND(MAX(TOTAL_COST_USD), 4)::VARCHAR AS max_query_cost,
                COUNT(DISTINCT USER_NAME)::VARCHAR AS unique_users,
                COUNT(DISTINCT WAREHOUSE_NAME)::VARCHAR AS unique_warehouses
            FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW
        ),
        anti_patterns AS (
            SELECT
                SUM(CASE WHEN HAS_ANTI_PATTERN THEN 1 ELSE 0 END)::VARCHAR AS anti_pattern_count,
                ROUND(SUM(CASE WHEN HAS_ANTI_PATTERN THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)::VARCHAR AS anti_pattern_pct
            FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW
        ),
        analysis_stats AS (
            SELECT
                COUNT(*)::VARCHAR AS total_analyzed,
                MAX(ANALYSIS_TIMESTAMP)::VARCHAR AS last_analysis_time
            FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS
        ),
        insight_stats AS (
            SELECT
                COUNT(*)::VARCHAR AS total_insights,
                MAX(ANALYSIS_DATE)::VARCHAR AS latest_insight_date
            FROM QUERY_OPTIMIZATION_DB.AGENT.AI_TREND_INSIGHTS
        )

        SELECT ''COST OVERVIEW'' AS METRIC_CATEGORY, ''Latest Data Date'' AS METRIC_NAME, latest_date AS METRIC_VALUE FROM cost_overview
        UNION ALL SELECT ''COST OVERVIEW'', ''Total Queries (Latest Day)'', total_queries FROM cost_overview
        UNION ALL SELECT ''COST OVERVIEW'', ''Total Cost USD (Latest Day)'', total_cost_usd FROM cost_overview
        UNION ALL SELECT ''COST OVERVIEW'', ''Total Credits (Latest Day)'', total_credits FROM cost_overview
        UNION ALL SELECT ''COST OVERVIEW'', ''Warehouses Tracked'', warehouses_tracked FROM cost_overview

        UNION ALL SELECT ''7-DAY TREND'', ''Total Cost (Last 7 Days)'', cost_last_7d FROM cost_7d
        UNION ALL SELECT ''7-DAY TREND'', ''Total Queries (Last 7 Days)'', queries_last_7d FROM cost_7d
        UNION ALL SELECT ''7-DAY TREND'', ''Avg Elapsed Time (Last 7 Days)'', avg_elapsed_7d FROM cost_7d

        UNION ALL SELECT ''BAD QUERIES'', ''Total Flagged Queries'', total_flagged_queries FROM bad_query_stats
        UNION ALL SELECT ''BAD QUERIES'', ''Queries with Disk Spill'', spill_queries FROM bad_query_stats
        UNION ALL SELECT ''BAD QUERIES'', ''Avg Cost per Bad Query (USD)'', avg_cost_per_bad_query FROM bad_query_stats
        UNION ALL SELECT ''BAD QUERIES'', ''Max Single Query Cost (USD)'', max_query_cost FROM bad_query_stats
        UNION ALL SELECT ''BAD QUERIES'', ''Unique Users'', unique_users FROM bad_query_stats
        UNION ALL SELECT ''BAD QUERIES'', ''Unique Warehouses'', unique_warehouses FROM bad_query_stats

        UNION ALL SELECT ''ANTI-PATTERNS'', ''Queries with Anti-Patterns'', anti_pattern_count FROM anti_patterns
        UNION ALL SELECT ''ANTI-PATTERNS'', ''Anti-Pattern Rate (%)'', anti_pattern_pct FROM anti_patterns

        UNION ALL SELECT ''ANALYSIS'', ''Total Queries Analyzed'', total_analyzed FROM analysis_stats
        UNION ALL SELECT ''ANALYSIS'', ''Last Analysis Run'', last_analysis_time FROM analysis_stats

        UNION ALL SELECT ''AI INSIGHTS'', ''Total AI Insights Generated'', total_insights FROM insight_stats
        UNION ALL SELECT ''AI INSIGHTS'', ''Latest Insight Date'', latest_insight_date FROM insight_stats
    );
    RETURN TABLE(res);
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.GET_TOP_EXPENSIVE_QUERIES("N" NUMBER(38,0) DEFAULT 10)
RETURNS TABLE ("QUERY_ID" VARCHAR, "USER_NAME" VARCHAR, "WAREHOUSE_NAME" VARCHAR, "WAREHOUSE_SIZE" VARCHAR, "TOTAL_COST_USD" VARCHAR, "TOTAL_CREDITS" VARCHAR, "TOTAL_ELAPSED_TIME_SEC" VARCHAR, "GB_SCANNED" VARCHAR, "SPILL_STATUS" VARCHAR, "ANTI_PATTERN_REASON" VARCHAR, "QUERY_TEXT" VARCHAR, "QUERY_DATE" VARCHAR)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'BEGIN
    LET res RESULTSET := (
        SELECT
            QUERY_ID::VARCHAR,
            USER_NAME::VARCHAR,
            WAREHOUSE_NAME::VARCHAR,
            WAREHOUSE_SIZE::VARCHAR,
            ROUND(TOTAL_COST_USD, 4)::VARCHAR AS TOTAL_COST_USD,
            ROUND(TOTAL_CREDITS, 4)::VARCHAR AS TOTAL_CREDITS,
            ROUND(TOTAL_ELAPSED_TIME_SEC, 2)::VARCHAR AS TOTAL_ELAPSED_TIME_SEC,
            ROUND(GB_SCANNED, 4)::VARCHAR AS GB_SCANNED,
            SPILL_STATUS::VARCHAR,
            ANTI_PATTERN_REASON::VARCHAR,
            LEFT(QUERY_TEXT, 500)::VARCHAR AS QUERY_TEXT,
            QUERY_DATE::VARCHAR
        FROM QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW_IT
        WHERE TOTAL_COST_USD IS NOT NULL
        ORDER BY TOTAL_COST_USD DESC
        LIMIT :N
    );
    RETURN TABLE(res);
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.LIST_USERS_WITH_ROLES()
RETURNS TABLE ("USER_NAME" VARCHAR, "ROLE_NAME" VARCHAR, "GRANTED_BY" VARCHAR, "CREATED_ON" VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN
    TRUNCATE TABLE QUERY_OPTIMIZATION_DB.AGENT.USER_ROLES;

    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.USER_ROLES
        SELECT
            u.NAME::VARCHAR,
            g.ROLE::VARCHAR,
            g.GRANTED_BY::VARCHAR,
            g.CREATED_ON::VARCHAR
        FROM SNOWFLAKE.ACCOUNT_USAGE.USERS u
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS g
            ON u.NAME = g.GRANTEE_NAME
        WHERE u.DELETED_ON IS NULL
          AND (g.DELETED_ON IS NULL)
        ORDER BY u.NAME, g.ROLE;

    LET res RESULTSET := (SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.USER_ROLES ORDER BY USER_NAME, ROLE_NAME);
    RETURN TABLE(res);
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.LIST_USERS_WITH_ROLES_V2()
RETURNS TABLE ("USER_NAME" VARCHAR, "ROLE_NAME" VARCHAR, "GRANTED_BY" VARCHAR, "CREATED_ON" VARCHAR)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'BEGIN
    LET res RESULTSET := (
        SELECT
            u.NAME::VARCHAR AS USER_NAME,
            g.ROLE::VARCHAR AS ROLE_NAME,
            g.GRANTED_BY::VARCHAR AS GRANTED_BY,
            g.CREATED_ON::VARCHAR AS CREATED_ON
        FROM SNOWFLAKE.ACCOUNT_USAGE.USERS u
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS g
            ON u.NAME = g.GRANTEE_NAME
        WHERE u.DELETED_ON IS NULL
          AND (g.DELETED_ON IS NULL OR g.DELETED_ON IS NULL)
        ORDER BY u.NAME, g.ROLE
    );
    RETURN TABLE(res);
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.PROCESS_ALERT_QUEUE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_count INTEGER := 0;
    cur CURSOR FOR
        SELECT ALERT_ID, EMAIL_TO, SUBJECT, BODY
        FROM QUERY_OPTIMIZATION_DB.AGENT.ALERT_PROCESSING;
    v_alert_id NUMBER;
    v_email VARCHAR;
    v_subject VARCHAR;
    v_body VARCHAR;
BEGIN
    TRUNCATE TABLE QUERY_OPTIMIZATION_DB.AGENT.ALERT_PROCESSING;

    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.ALERT_PROCESSING (ALERT_ID, EMAIL_TO, SUBJECT, BODY)
    SELECT ALERT_ID, EMAIL_TO, SUBJECT, BODY
    FROM QUERY_OPTIMIZATION_DB.AGENT.ALERT_QUEUE_STREAM
    WHERE METADATA$ACTION = ''INSERT'';

    FOR rec IN cur DO
        v_alert_id := rec.ALERT_ID;
        v_email := rec.EMAIL_TO;
        v_subject := rec.SUBJECT;
        v_body := rec.BODY;

        CALL SYSTEM$SEND_EMAIL(
            ''QUERY_OPTIMIZATION_EMAIL_INT'',
            :v_email,
            :v_subject,
            :v_body
        );

        UPDATE QUERY_OPTIMIZATION_DB.AGENT.ALERT_QUEUE
        SET IS_SENT = TRUE
        WHERE ALERT_ID = :v_alert_id;

        v_count := v_count + 1;
    END FOR;

    RETURN ''Sent '' || v_count || '' alert email(s).'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.READ_GIT_FILE("FILE_PATH" VARCHAR)
RETURNS TABLE ("CONTENT" VARCHAR)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  query STRING;
BEGIN
  query := ''SELECT $1 AS content FROM @QUERY_OPTIMIZATION_DB.AGENT.MY_REPO/branches/develop/'' || :file_path || '' (FILE_FORMAT => ''''QUERY_OPTIMIZATION_DB.AGENT.TMP_TEXT_FORMAT'''')'';
  LET rs RESULTSET := (EXECUTE IMMEDIATE :query);
  RETURN TABLE(rs);
END';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.RUN_OPTIMIZATION_AGENT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    result VARCHAR;
BEGIN
    SELECT TRY_PARSE_JSON(
        SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
            ''QUERY_OPTIMIZATION_DB.AGENT.QUERY_OPTIMIZER_AGENT'',
            ''{"messages": [{"role": "user", "content": [{"type": "text", "text": "Analyze all unanalyzed bad queries and save analysis for each."}]}], "stream": false}''
        )
    ):content[0]:text::VARCHAR INTO result;
    
    RETURN COALESCE(result, ''Agent execution completed'');
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.RUN_TREND_ANALYSIS("WAREHOUSE_FILTER" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    result VARCHAR;
BEGIN
    -- Step 1: Collect daily cost trends
    CALL QUERY_OPTIMIZATION_DB.AGENT.COLLECT_DAILY_COST_TRENDS(30);
    
    -- Step 2: Detect peak patterns
    CALL QUERY_OPTIMIZATION_DB.AGENT.DETECT_PEAK_PATTERNS();
    
    -- Step 4: Generate AI insights
    CALL QUERY_OPTIMIZATION_DB.AGENT.GENERATE_AI_TREND_INSIGHTS(:warehouse_filter);
    
    RETURN ''Trend analysis completed successfully'';
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.SAVE_ANALYSIS("P_CAPTURE_ID" NUMBER(38,0), "P_QUERY_ID" VARCHAR, "P_KEY_FINDINGS" VARCHAR, "P_OPTIMIZATION_SUGGESTIONS" VARCHAR, "P_SUGGESTED_REWRITE" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS (
        CAPTURE_ID, QUERY_ID, KEY_FINDINGS, OPTIMIZATION_SUGGESTIONS, SUGGESTED_QUERY_REWRITE
    ) VALUES (
        P_CAPTURE_ID, P_QUERY_ID, P_KEY_FINDINGS, P_OPTIMIZATION_SUGGESTIONS, P_SUGGESTED_REWRITE
    );
    
    UPDATE QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW
    SET IS_ANALYZED = TRUE
    WHERE CAPTURE_ID = P_CAPTURE_ID;
    
    RETURN ''Analysis saved for query '' || P_QUERY_ID;
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.SEND_ALERT("P_USER_NAME" VARCHAR, "P_QUERY_IDS" ARRAY)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_email VARCHAR;
    v_default_email VARCHAR DEFAULT ''sarkar.sudipta1976@gmail.com'';
    v_subject VARCHAR;
    v_body VARCHAR;
    v_query_count INTEGER;
BEGIN
    SELECT EMAIL_ADDRESS INTO v_email
    FROM QUERY_OPTIMIZATION_DB.AGENT.USER_EMAIL_MAPPING
    WHERE USER_NAME = :P_USER_NAME;

    IF (v_email IS NULL) THEN
        v_email := v_default_email;
    END IF;

    v_query_count := ARRAY_SIZE(:P_QUERY_IDS);

    v_subject := ''Query Optimization Alert for '' || :P_USER_NAME || '' - '' || v_query_count || '' Queries Flagged'';

    SELECT
        ''Hello '' || :P_USER_NAME || '',\\n\\n'' ||
        ''The following '' || :v_query_count || '' query(ies) have been flagged for optimization:\\n\\n'' ||
        LISTAGG(
            ''-----------------------------\\n'' ||
            ''Query ID: '' || r.QUERY_ID || ''\\n'' ||
            ''Cost: $'' || ROUND(b.TOTAL_COST_USD, 2)::VARCHAR || ''\\n'' ||
            ''Runtime: '' || ROUND(b.TOTAL_ELAPSED_TIME_SEC, 1)::VARCHAR || ''s\\n'' ||
            ''Findings: '' || LEFT(r.KEY_FINDINGS, 300) || ''\\n'' ||
            ''Suggestion: '' || LEFT(r.OPTIMIZATION_SUGGESTIONS, 300) || ''\\n'',
            ''\\n''
        ) WITHIN GROUP (ORDER BY b.TOTAL_COST_USD DESC) ||
        ''\\n-----------------------------\\n'' ||
        ''Review full details:\\n'' ||
        ''SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS WHERE QUERY_ID IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(PARSE_JSON('''''' || :P_QUERY_IDS::VARCHAR || ''''''))));\\n\\n'' ||
        ''Regards,\\nQuery Optimization System''
    INTO v_body
    FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS r
    JOIN QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW b ON r.CAPTURE_ID = b.CAPTURE_ID
    WHERE r.QUERY_ID IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(:P_QUERY_IDS)));

    IF (v_body IS NULL) THEN
        RETURN ''No analysis results found for the provided query IDs.'';
    END IF;

    CALL SYSTEM$SEND_EMAIL(
        ''QUERY_OPTIMIZATION_EMAIL_INT'',
        :v_email,
        :v_subject,
        :v_body
    );

    RETURN ''Alert sent to '' || v_email || '' for '' || v_query_count || '' queries.'';

EXCEPTION
    WHEN OTHER THEN
        RETURN ''Error sending alert: '' || SQLERRM;
END;
';

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZATION_DB.AGENT.SEND_ALERT("P_USER_NAME" VARCHAR, "P_QUERY_IDS" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_email VARCHAR;
    v_default_email VARCHAR DEFAULT ''sarkar.sudipta1976@gmail.com'';
    v_subject VARCHAR;
    v_body VARCHAR;
    v_query_count INTEGER;
BEGIN
    SELECT EMAIL_ADDRESS INTO v_email
    FROM QUERY_OPTIMIZATION_DB.AGENT.USER_EMAIL_MAPPING
    WHERE USER_NAME = :P_USER_NAME
    LIMIT 1;

    IF (v_email IS NULL) THEN
        v_email := v_default_email;
    END IF;

    SELECT ARRAY_SIZE(SPLIT(:P_QUERY_IDS, ''/'')) INTO v_query_count;

    v_subject := ''Query Optimization Alert for '' || :P_USER_NAME || '' - '' || v_query_count || '' Queries Flagged'';

    SELECT
        ''Hello '' || :P_USER_NAME || '',\\n\\n'' ||
        ''The following '' || :v_query_count || '' query(ies) have been flagged for optimization:\\n\\n'' ||
        LISTAGG(
            ''-----------------------------\\n'' ||
            ''Query ID: '' || r.QUERY_ID || ''\\n'' ||
            ''Cost: $'' || ROUND(b.TOTAL_COST_USD, 2)::VARCHAR || ''\\n'' ||
            ''Runtime: '' || ROUND(b.TOTAL_ELAPSED_TIME_SEC, 1)::VARCHAR || ''s\\n'' ||
            ''Findings: '' || LEFT(r.KEY_FINDINGS, 300) || ''\\n'' ||
            ''Suggestion: '' || LEFT(r.OPTIMIZATION_SUGGESTIONS, 300) || ''\\n'',
            ''\\n''
        ) WITHIN GROUP (ORDER BY b.TOTAL_COST_USD DESC) ||
        ''\\n-----------------------------\\n'' ||
        ''Review full details:\\n'' ||
        ''SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS WHERE QUERY_ID IN (SELECT TRIM(VALUE)::VARCHAR FROM TABLE(FLATTEN(INPUT => SPLIT('''''' || :P_QUERY_IDS || '''''', ''''/''''))));\\n\\n'' ||
        ''Regards,\\nQuery Optimization System''
    INTO v_body
    FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS r
    JOIN QUERY_OPTIMIZATION_DB.AGENT.BAD_QUERIES_RAW b ON r.CAPTURE_ID = b.CAPTURE_ID
    WHERE r.QUERY_ID IN (SELECT TRIM(VALUE::VARCHAR) FROM TABLE(FLATTEN(INPUT => SPLIT(:P_QUERY_IDS, ''/''))))
    LIMIT 1;

    IF (v_body IS NULL) THEN
        RETURN ''No analysis results found for the provided query IDs.'';
    END IF;

    INSERT INTO QUERY_OPTIMIZATION_DB.AGENT.ALERT_QUEUE (EMAIL_TO, SUBJECT, BODY)
    VALUES (:v_email, :v_subject, :v_body);

    RETURN ''Alert queued for '' || v_email || '' for '' || v_query_count || '' queries. Email will be sent shortly.'';

EXCEPTION
    WHEN OTHER THEN
        RETURN ''Error sending alert: '' || SQLERRM;
END;
';



