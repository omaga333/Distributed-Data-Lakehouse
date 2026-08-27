{{ config(materialized='table', tags=['silver']) }}

-- Silver layer: Payment transaction analysis with fraud detection features
WITH payment_enriched AS (
    SELECT
        transaction_id,
        order_id,
        customer_id,
        payment_method,
        payment_status,
        amount,
        currency,
        transaction_timestamp,
        processor_response_code,
        gateway_fee,
        merchant_id,
        billing_country,
        risk_score,

        -- Time-based features
        EXTRACT(HOUR FROM transaction_timestamp) as transaction_hour,
        EXTRACT(DOW FROM transaction_timestamp) as transaction_day_of_week,
        DATE(transaction_timestamp) as transaction_date,

        -- Payment success indicator
        CASE
            WHEN payment_status = 'completed' THEN TRUE
            ELSE FALSE
        END as is_successful,

        -- High-risk transaction indicator
        CASE
            WHEN risk_score >= 80 THEN TRUE
            ELSE FALSE
        END as is_high_risk,

        -- Failed payment indicator
        CASE
            WHEN payment_status = 'failed' THEN TRUE
            ELSE FALSE
        END as is_failed,

        -- Refunded transaction indicator
        CASE
            WHEN payment_status = 'refunded' THEN TRUE
            ELSE FALSE
        END as is_refunded,

        -- Net payment amount (after gateway fees)
        amount - gateway_fee as net_amount

    FROM {{ ref('bronze_payment_transactions') }}
),

customer_payment_patterns AS (
    SELECT
        customer_id,
        COUNT(*) as total_transactions,
        COUNT(CASE WHEN is_successful THEN 1 END) as successful_transactions,
        COUNT(CASE WHEN is_failed THEN 1 END) as failed_transactions,
        COUNT(CASE WHEN is_high_risk THEN 1 END) as high_risk_transactions,
        AVG(amount) as avg_transaction_amount,
        MAX(amount) as max_transaction_amount,
        SUM(CASE WHEN is_successful THEN amount ELSE 0 END) as total_successful_amount,
        COUNT(DISTINCT payment_method) as unique_payment_methods
    FROM payment_enriched
    GROUP BY customer_id
)

SELECT
    p.transaction_id,
    p.order_id,
    p.customer_id,
    p.payment_method,
    p.payment_status,
    p.amount,
    p.currency,
    p.transaction_timestamp,
    p.transaction_hour,
    p.transaction_day_of_week,
    p.transaction_date,
    p.processor_response_code,
    p.gateway_fee,
    p.net_amount,
    p.merchant_id,
    p.billing_country,
    p.risk_score,
    p.is_successful,
    p.is_high_risk,
    p.is_failed,
    p.is_refunded,

    -- Customer behavior features
    cp.total_transactions as customer_total_transactions,
    cp.successful_transactions as customer_successful_transactions,
    cp.failed_transactions as customer_failed_transactions,
    cp.avg_transaction_amount as customer_avg_amount,
    cp.max_transaction_amount as customer_max_amount,

    -- Customer risk indicators
    CASE
        WHEN CAST(cp.failed_transactions AS DOUBLE) / NULLIF(cp.total_transactions, 0) > 0.3 THEN TRUE
        ELSE FALSE
    END as is_high_failure_rate_customer,

    CASE
        WHEN p.amount > cp.avg_transaction_amount * 3 THEN TRUE
        ELSE FALSE
    END as is_unusually_large_transaction,

    -- Payment method risk classification
    CASE
        WHEN p.payment_method IN ('credit_card', 'debit_card') THEN 'Low Risk'
        WHEN p.payment_method IN ('paypal', 'apple_pay', 'google_pay') THEN 'Medium Risk'
        ELSE 'High Risk'
    END as payment_method_risk_category,

    CURRENT_TIMESTAMP as processed_at

FROM payment_enriched p
LEFT JOIN customer_payment_patterns cp ON p.customer_id = cp.customer_id