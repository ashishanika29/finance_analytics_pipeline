/*
    MODEL: int_orders_cleansed
    LAYER: Silver (Intermediate)
    OBJECTIVE: 
        1. Cast order_date to DATE.
        2. Standardize order_amount to a numeric decimal.
        3. Clean and uppercase status codes.
*/

{{
    config(
        materialized='table',
        schema='SILVER_TRANFORMATION'
    )
}}

-- STEP 1: Import from the Bronze view using ref()
with staging_data as (
    select * from {{ ref('stg_orders') }}
),

-- STEP 2: Apply cleansing, formatting, and business logic
cleansed_data as (
    select
        order_id,
        customer_id,
        order_date::date as order_date,
        -- LOGIC: Handle nulls, cast to decimal, AND ensure no negative values
        case 
            when coalesce(amount, 0) < 0 then 0 
            else coalesce(amount, 0)
        end::decimal(10,2) as order_amount_usd,
        upper(trim(status)) as order_status
    from staging_data
    -- THE FIX: Only keep orders where the customer exists in our cleansed customer table
    where customer_id in (select customer_id from {{ ref('int_customers_cleansed') }})
)

select * from cleansed_data