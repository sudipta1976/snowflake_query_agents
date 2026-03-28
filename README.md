# snowflake_query_agents
Snowflake auto healing and alert agents and intelligence
# Snowflake Query Optimization and Alerting Agents System

## Overview
The Snowflake Query Optimization and Alerting Agents is a sophisticated system designed to enhance the performance of queries executed in Snowflake environments by utilizing intelligent algorithms and AI-powered analysis. This system helps in monitoring, optimizing, and alerting stakeholders regarding query execution and performance.

## System Architecture

### Three-Tier Design

**Tier 1: Raw Query Collection (Hourly, No LLM Cost)**
- Hourly automated collection of underperforming queries from Snowflake Account Usage
- Filters queries based on cost, execution time, spills, and partition scan percentage
- Initial AI_FILTER step to identify queries with potential anti-patterns
- Stores raw query data with performance metrics in `BAD_QUERIES_RAW` table

**Tier 2: AI-Powered Analysis (Daily)**
- Daily analysis of unanalyzed queries using Snowflake Cortex AI
- Generates key findings on performance issues
- Provides 2-3 actionable optimization suggestions
- Creates optimized SQL query rewrites
- Uses mistral-large2 LLM for intelligent analysis

**Tier 3: Trend Analytics & Intelligence (Daily)**
- Collects daily cost trends with 7-day moving averages
- Detects peak usage hours and off-peak hours by warehouse
- Generates AI-powered trend insights and recommendations
- Analyzes cost changes, spill patterns, and performance anomalies

## Key Tables

### Core Tables
- **BAD_QUERIES_RAW**: Raw performance-flagged queries with comprehensive metrics
- **QUERY_ANALYSIS_RESULTS**: AI analysis findings and optimization suggestions
- **DAILY_COST_TRENDS**: Warehouse cost trends with statistical analysis
- **PEAK_HOUR_PATTERNS**: Usage patterns, peak hours, and weekly distribution
- **AI_TREND_INSIGHTS**: AI-generated insights and recommendations

### Supporting Tables
- **ALERT_QUEUE**: Email alert queue for notification processing
- **USER_EMAIL_MAPPING**: User-to-email mappings for alerting
- **USER_ROLES**: User role tracking and management

## Performance Metrics Tracked

Each query is analyzed for:
- **Execution Metrics**: Elapsed time, execution status, runtime
- **Cost Metrics**: Total cost in USD, credits consumed, cost per query
- **Data Access Metrics**: GB scanned, cache hit percentage, partition scan percentage
- **Resource Metrics**: Warehouse size, warehouse used, query type
- **Spill Status**: Local spill, remote spill detection
- **Queue Status**: Query queuing during warehouse overload

## Key Procedures

### Collection & Analysis
- `COLLECT_BAD_QUERIES()` - Hourly bad query collection
- `ANALYZE_AND_SAVE_QUERIES()` - Daily AI analysis
- `COLLECT_DAILY_COST_TRENDS(lookback_days)` - Daily cost trend collection
- `DETECT_PEAK_PATTERNS()` - Weekly/daily pattern detection
- `GENERATE_AI_TREND_INSIGHTS()` - AI insight generation

### Reporting & Intelligence
- `GET_OPTIMIZATION_SUMMARY()` - Comprehensive metrics summary
- `GET_TOP_EXPENSIVE_QUERIES(N)` - Top expensive queries
- `RUN_TREND_ANALYSIS()` - Full trend analysis pipeline
- `LIST_USERS_WITH_ROLES()` - User and role tracking

### Alerting
- `SEND_ALERT()` - Manual alert triggering
- `NEW_BAD_QUERIES_ALERT` - Scheduled daily alert
- Automated email notifications at 7 AM UTC

## Scheduled Tasks

| Task | Schedule | Purpose |
|------|----------|---------|
| COLLECT_BAD_QUERIES_TASK | Every 60 minutes | Hourly bad query collection |
| RUN_ANALYSIS_DAILY_TASK | 6 AM UTC daily | Daily AI analysis execution |
| NEW_BAD_QUERIES_ALERT | 7 AM UTC daily | Email alert notification |

## AI Integration

### Models Used
- **AI_FILTER**: Fast pattern detection (llama3.1-8b)
- **SNOWFLAKE.CORTEX.COMPLETE**: Advanced analysis (mistral-large2)

### Anti-Pattern Detection
The system identifies common SQL anti-patterns including:
- SELECT * usage
- Missing or weak WHERE clauses
- JOIN inefficiencies (CROSS JOINs)
- Non-sargable predicates
- Spilling risk patterns
- Redundant subqueries
- Lack of partition pruning

## Setup Instructions

### Prerequisites
- Snowflake account with appropriate permissions
- Cortex AI enabled in your region
- Compute warehouse configured
- Email integration configured

### Deployment Steps
1. Create database and schema
2. Deploy tables using tables.sql
3. Deploy procedures using procedure.sql and agents.sql
4. Configure email notifications
5. Enable and resume tasks

## Usage Examples

```sql
-- View Recent Analysis
SELECT * FROM QUERY_OPTIMIZATION_DB.AGENT.QUERY_ANALYSIS_RESULTS 
WHERE DATE(ANALYSIS_TIMESTAMP) = CURRENT_DATE()
ORDER BY ANALYSIS_TIMESTAMP DESC;

-- Get Top Expensive Queries
CALL QUERY_OPTIMIZATION_DB.AGENT.GET_TOP_EXPENSIVE_QUERIES(10);

-- Get Optimization Summary
CALL QUERY_OPTIMIZATION_DB.AGENT.GET_OPTIMIZATION_SUMMARY();


This comprehensive README covers your system's architecture, components, setup, and usage patterns based on the detailed SQL code in your repository.
