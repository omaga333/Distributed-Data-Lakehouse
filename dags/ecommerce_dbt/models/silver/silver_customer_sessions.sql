{{ config(materialized='table', tags=['silver']) }}

-- Silver layer: Customer session analysis with event aggregations
WITH session_events AS (
    SELECT
        customer_id,
        session_id,
        MIN(event_timestamp) as session_start,
        MAX(event_timestamp) as session_end,
        COUNT(*) as total_events,
        COUNT(DISTINCT event_type) as unique_event_types,
        COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as page_views,
        COUNT(CASE WHEN event_type = 'product_view' THEN 1 END) as product_views,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as cart_adds,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
        COUNT(DISTINCT product_id) as unique_products_viewed,
        device_type,
        referrer_source
    FROM {{ ref('bronze_customer_events') }}
    GROUP BY customer_id, session_id, device_type, referrer_source
),

session_metrics AS (
    SELECT
        *,
        DATE_DIFF('second', session_start, session_end) as session_duration_seconds,
        CASE
            WHEN purchases > 0 THEN TRUE
            ELSE FALSE
        END as converted,
        CASE
            WHEN cart_adds > 0 AND purchases = 0 THEN TRUE
            ELSE FALSE
        END as abandoned_cart,
        CASE
            WHEN total_events = 1 THEN TRUE
            ELSE FALSE
        END as bounce_session
    FROM session_events
)

SELECT
    customer_id,
    session_id,
    session_start,
    session_end,
    session_duration_seconds,
    CASE
        WHEN session_duration_seconds < 30 THEN 'Very Short'
        WHEN session_duration_seconds < 300 THEN 'Short'
        WHEN session_duration_seconds < 1800 THEN 'Medium'
        ELSE 'Long'
    END as session_length_category,
    total_events,
    unique_event_types,
    page_views,
    product_views,
    cart_adds,
    purchases,
    unique_products_viewed,
    converted,
    abandoned_cart,
    bounce_session,
    device_type,
    referrer_source,
    CURRENT_TIMESTAMP as processed_at
FROM session_metrics