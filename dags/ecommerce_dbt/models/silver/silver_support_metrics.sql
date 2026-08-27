{{ config(materialized='table', tags=['silver']) }}

-- Silver layer: Support ticket analysis with SLA tracking and agent performance
WITH ticket_enriched AS (
    SELECT
        ticket_id,
        customer_id,
        order_id,
        ticket_type,
        priority,
        status,
        created_timestamp,
        first_response_timestamp,
        resolution_timestamp,
        agent_id,
        satisfaction_score,
        subject,
        channel,

        -- Response time calculations (in minutes)
        CASE
            WHEN first_response_timestamp IS NOT NULL THEN
                DATE_DIFF('minute', created_timestamp, first_response_timestamp)
            ELSE NULL
        END as first_response_time_minutes,

        -- Resolution time calculations (in hours)
        CASE
            WHEN resolution_timestamp IS NOT NULL THEN
                DATE_DIFF('hour', created_timestamp, resolution_timestamp)
            ELSE NULL
        END as resolution_time_hours,

        -- SLA compliance indicators
        CASE
            WHEN priority = 'urgent' AND first_response_timestamp IS NOT NULL
                AND DATE_DIFF('minute', created_timestamp, first_response_timestamp) <= 15 THEN TRUE
            WHEN priority = 'high' AND first_response_timestamp IS NOT NULL
                AND DATE_DIFF('minute', created_timestamp, first_response_timestamp) <= 60 THEN TRUE
            WHEN priority = 'medium' AND first_response_timestamp IS NOT NULL
                AND DATE_DIFF('hour', created_timestamp, first_response_timestamp) <= 4 THEN TRUE
            WHEN priority = 'low' AND first_response_timestamp IS NOT NULL
                AND DATE_DIFF('hour', created_timestamp, first_response_timestamp) <= 24 THEN TRUE
            ELSE FALSE
        END as meets_response_sla,

        CASE
            WHEN priority = 'urgent' AND resolution_timestamp IS NOT NULL
                AND DATE_DIFF('hour', created_timestamp, resolution_timestamp) <= 4 THEN TRUE
            WHEN priority = 'high' AND resolution_timestamp IS NOT NULL
                AND DATE_DIFF('hour', created_timestamp, resolution_timestamp) <= 24 THEN TRUE
            WHEN priority = 'medium' AND resolution_timestamp IS NOT NULL
                AND DATE_DIFF('hour', created_timestamp, resolution_timestamp) <= 72 THEN TRUE
            WHEN priority = 'low' AND resolution_timestamp IS NOT NULL
                AND DATE_DIFF('hour', created_timestamp, resolution_timestamp) <= 168 THEN TRUE
            ELSE FALSE
        END as meets_resolution_sla,

        -- Ticket age for open tickets
        CASE
            WHEN status IN ('open', 'in_progress') THEN
                DATE_DIFF('hour', created_timestamp, CURRENT_TIMESTAMP)
            ELSE NULL
        END as current_age_hours,

        -- Customer satisfaction indicators
        CASE
            WHEN satisfaction_score IS NOT NULL AND satisfaction_score >= 4 THEN TRUE
            ELSE FALSE
        END as is_satisfied_customer,

        CASE
            WHEN satisfaction_score IS NOT NULL AND satisfaction_score <= 2 THEN TRUE
            ELSE FALSE
        END as is_unsatisfied_customer

    FROM {{ ref('bronze_support_tickets') }}
),

agent_performance AS (
    SELECT
        agent_id,
        COUNT(*) as total_tickets_handled,
        AVG(first_response_time_minutes) as avg_first_response_time,
        AVG(resolution_time_hours) as avg_resolution_time,
        COUNT(CASE WHEN meets_response_sla THEN 1 END) as response_sla_met_count,
        COUNT(CASE WHEN meets_resolution_sla THEN 1 END) as resolution_sla_met_count,
        AVG(satisfaction_score) as avg_satisfaction_score,
        COUNT(CASE WHEN is_satisfied_customer THEN 1 END) as satisfied_customers,
        COUNT(CASE WHEN is_unsatisfied_customer THEN 1 END) as unsatisfied_customers
    FROM ticket_enriched
    WHERE agent_id IS NOT NULL
    GROUP BY agent_id
)

SELECT
    t.ticket_id,
    t.customer_id,
    t.order_id,
    t.ticket_type,
    t.priority,
    t.status,
    t.created_timestamp,
    t.first_response_timestamp,
    t.resolution_timestamp,
    t.agent_id,
    t.satisfaction_score,
    t.subject,
    t.channel,
    t.first_response_time_minutes,
    t.resolution_time_hours,
    t.meets_response_sla,
    t.meets_resolution_sla,
    t.current_age_hours,
    t.is_satisfied_customer,
    t.is_unsatisfied_customer,

    -- Agent performance context
    a.avg_first_response_time as agent_avg_response_time,
    a.avg_resolution_time as agent_avg_resolution_time,
    a.avg_satisfaction_score as agent_avg_satisfaction,

    -- Ticket urgency classification
    CASE
        WHEN t.priority = 'urgent' AND t.current_age_hours > 4 THEN 'Overdue Critical'
        WHEN t.priority = 'high' AND t.current_age_hours > 24 THEN 'Overdue High'
        WHEN t.priority = 'medium' AND t.current_age_hours > 72 THEN 'Overdue Medium'
        WHEN t.priority = 'low' AND t.current_age_hours > 168 THEN 'Overdue Low'
        WHEN t.status IN ('open', 'in_progress') THEN 'Active'
        ELSE 'Resolved'
    END as ticket_urgency_status,

    -- Channel efficiency indicator
    CASE
        WHEN t.channel = 'chat' THEN 'Real-time'
        WHEN t.channel = 'phone' THEN 'Real-time'
        WHEN t.channel = 'email' THEN 'Asynchronous'
        WHEN t.channel = 'social_media' THEN 'Public'
        ELSE 'Other'
    END as channel_type,

    CURRENT_TIMESTAMP as processed_at

FROM ticket_enriched t
LEFT JOIN agent_performance a ON t.agent_id = a.agent_id