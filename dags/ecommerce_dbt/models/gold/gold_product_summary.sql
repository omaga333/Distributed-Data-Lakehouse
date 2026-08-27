{{ config(materialized='table', tags=['gold']) }}

-- Simple gold layer product summary
WITH product_events AS (
    SELECT
        product_id,
        COUNT(*) as total_events,
        COUNT(DISTINCT customer_id) as unique_viewers,
        COUNT(DISTINCT session_id) as unique_sessions
    FROM {{ ref('bronze_customer_events') }}
    WHERE product_id IS NOT NULL
    GROUP BY product_id
),

product_inventory AS (
    SELECT
        product_id,
        AVG(quantity_on_hand) as avg_stock_level,
        AVG(reorder_point) as avg_reorder_point,
        MAX(needs_reorder) as ever_out_of_stock
    FROM {{ ref('silver_inventory_health') }}
    GROUP BY product_id
)

SELECT
    COALESCE(pe.product_id, pi.product_id) as product_id,

    -- Event metrics
    COALESCE(pe.total_events, 0) as total_events,
    COALESCE(pe.unique_viewers, 0) as unique_viewers,
    COALESCE(pe.unique_sessions, 0) as unique_sessions,

    -- Inventory metrics
    COALESCE(pi.avg_stock_level, 0) as avg_stock_level,
    COALESCE(pi.avg_reorder_point, 0) as avg_reorder_point,
    COALESCE(pi.ever_out_of_stock, FALSE) as ever_out_of_stock,

    CURRENT_TIMESTAMP as calculated_at

FROM product_events pe
FULL OUTER JOIN product_inventory pi ON pe.product_id = pi.product_id