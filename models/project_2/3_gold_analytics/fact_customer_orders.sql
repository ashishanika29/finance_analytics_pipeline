{{ config(
    materialized='table',
    schema='GOLD_ANALYTICS'
) }}


/*
    MODEL: fact_customer_orders
    LAYER: Gold (Marts)
    OBJECTIVE: Final reporting table joining customers and orders 
               to provide a single source of truth for revenue.
*/

with customers as (
    select * from {{ ref('int_customers_cleansed') }}
),

orders as (
    select * from {{ ref('int_orders_cleansed') }}
),

final_join as (
    select
        o.order_id,
        o.order_date,
        o.order_amount_usd,
        o.order_status,
        c.customer_id,
        -- Bringing in descriptive fields for Power BI
        c.customer_name,
        -- Example of a Gold-layer business calculation
        case 
            when o.order_amount_usd > 200 then 'High Value'
            else 'Standard'
        end as order_tier
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final_join