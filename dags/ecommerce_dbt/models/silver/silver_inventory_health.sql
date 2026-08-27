{{ config(materialized='table', tags=['silver']) }}

-- Silver layer: Inventory health analysis with stock level classifications
WITH latest_inventory AS (
    SELECT
        product_id,
        warehouse_id,
        snapshot_date,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        supplier_id,
        unit_cost,
        ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY snapshot_date DESC) as rn
    FROM {{ ref('bronze_inventory_snapshots') }}
),

inventory_metrics AS (
    SELECT
        product_id,
        warehouse_id,
        snapshot_date,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        supplier_id,
        unit_cost,

        -- Stock health classifications
        CASE
            WHEN quantity_available <= 0 THEN 'Out of Stock'
            WHEN quantity_available <= reorder_point THEN 'Low Stock'
            WHEN quantity_available <= reorder_point * 2 THEN 'Medium Stock'
            ELSE 'High Stock'
        END as stock_status,

        -- Days of inventory calculation
        CASE
            WHEN quantity_available > 0 THEN
                CAST(quantity_available AS DOUBLE) / GREATEST(1, reorder_point) * 30
            ELSE 0
        END as estimated_days_of_inventory,

        -- Reorder recommendation
        CASE
            WHEN quantity_available <= reorder_point THEN TRUE
            ELSE FALSE
        END as needs_reorder,

        -- Stock turn rate estimate (simplified)
        CASE
            WHEN quantity_on_hand > 0 THEN
                CAST(reorder_quantity AS DOUBLE) / quantity_on_hand * 12
            ELSE 0
        END as estimated_annual_turns,

        -- Inventory value
        quantity_on_hand * unit_cost as inventory_value,
        quantity_available * unit_cost as available_inventory_value

    FROM latest_inventory
    WHERE rn = 1
)

SELECT
    product_id,
    warehouse_id,
    snapshot_date,
    quantity_on_hand,
    quantity_reserved,
    quantity_available,
    reorder_point,
    reorder_quantity,
    supplier_id,
    unit_cost,
    stock_status,
    estimated_days_of_inventory,
    needs_reorder,
    estimated_annual_turns,
    inventory_value,
    available_inventory_value,

    -- Risk classifications
    CASE
        WHEN stock_status = 'Out of Stock' THEN 'Critical'
        WHEN stock_status = 'Low Stock' AND estimated_days_of_inventory < 7 THEN 'High'
        WHEN stock_status = 'Low Stock' THEN 'Medium'
        ELSE 'Low'
    END as inventory_risk_level,

    CURRENT_TIMESTAMP as processed_at
FROM inventory_metrics