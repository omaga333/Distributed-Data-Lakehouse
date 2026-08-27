{{ config(materialized='table', tags=['gold']) }}

-- Simple gold layer daily metrics
WITH daily_sessions AS (
    SELECT
        DATE(session_start) as metric_date,
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(DISTINCT customer_id) as unique_visitors,
        AVG(session_duration_seconds) as avg_session_duration
    FROM {{ ref('silver_customer_sessions') }}
    GROUP BY DATE(session_start)
),

daily_payments AS (
    SELECT
        DATE(transaction_timestamp) as metric_date,
        COUNT(*) as total_transactions,
        COUNT(CASE WHEN is_successful THEN 1 END) as successful_transactions,
        SUM(CASE WHEN is_successful THEN amount ELSE 0 END) as daily_revenue,
        AVG(risk_score) as avg_risk_score
    FROM {{ ref('silver_payment_analysis') }}
    GROUP BY DATE(transaction_timestamp)
),

daily_support AS (
    SELECT
        DATE(created_timestamp) as metric_date,
        COUNT(*) as total_tickets,
        AVG(first_response_time_minutes/60.0) as avg_response_time,
        AVG(resolution_time_hours) as avg_resolution_time
    FROM {{ ref('silver_support_metrics') }}
    GROUP BY DATE(created_timestamp)
)

SELECT
    COALESCE(ds.metric_date, dp.metric_date, dsu.metric_date) as metric_date,

    -- Session metrics
    COALESCE(ds.total_sessions, 0) as total_sessions,
    COALESCE(ds.unique_visitors, 0) as unique_visitors,
    COALESCE(ds.avg_session_duration, 0) as avg_session_duration,

    -- Payment metrics
    COALESCE(dp.total_transactions, 0) as total_transactions,
    COALESCE(dp.successful_transactions, 0) as successful_transactions,
    COALESCE(dp.daily_revenue, 0) as daily_revenue,
    COALESCE(dp.avg_risk_score, 0) as avg_risk_score,

    -- Support metrics
    COALESCE(dsu.total_tickets, 0) as total_tickets,
    COALESCE(dsu.avg_response_time, 0) as avg_response_time,
    COALESCE(dsu.avg_resolution_time, 0) as avg_resolution_time,

    CURRENT_TIMESTAMP as calculated_at

FROM daily_sessions ds
FULL OUTER JOIN daily_payments dp ON ds.metric_date = dp.metric_date
FULL OUTER JOIN daily_support dsu ON COALESCE(ds.metric_date, dp.metric_date) = dsu.metric_date