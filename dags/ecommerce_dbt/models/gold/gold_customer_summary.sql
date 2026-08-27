{{ config(materialized='table', tags=['gold']) }}

-- Simple gold layer customer summary
SELECT
    cs.customer_id,

    -- Session metrics
    COUNT(DISTINCT cs.session_id) as total_sessions,
    AVG(cs.session_duration_seconds) as avg_session_duration,
    SUM(cs.total_events) as total_events,

    -- Payment metrics
    COUNT(DISTINCT pa.transaction_id) as total_transactions,
    SUM(CASE WHEN pa.is_successful THEN pa.amount ELSE 0 END) as total_revenue,
    AVG(CASE WHEN pa.is_successful THEN pa.amount END) as avg_transaction_amount,

    -- Support metrics
    COUNT(DISTINCT sm.ticket_id) as total_support_tickets,
    AVG(sm.satisfaction_score) as avg_satisfaction_score,

    CURRENT_TIMESTAMP as calculated_at

FROM {{ ref('silver_customer_sessions') }} cs
LEFT JOIN {{ ref('silver_payment_analysis') }} pa ON cs.customer_id = pa.customer_id
LEFT JOIN {{ ref('silver_support_metrics') }} sm ON cs.customer_id = sm.customer_id
GROUP BY cs.customer_id