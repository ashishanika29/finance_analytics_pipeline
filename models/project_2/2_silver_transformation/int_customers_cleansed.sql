/*
    MODEL: int_customers_cleansed
    LAYER: Silver (Intermediate)
    OBJECTIVE: 
        1. Clean whitespace from names.
        2. Standardize email to lowercase.
        3. Cast signup_date to a true DATE type.
*/

{{
    config(
        materialized='table'
    )
}}

-- STEP 1: Import from the Bronze view using ref()
with staging_data as (
    select * from {{ ref('stg_customers') }}
),

-- STEP 2: Apply cleaning and formatting logic
cleansed_data as (
    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as customer_name,
        lower(trim(email)) as email,
        signup_date::date as signup_date
    from staging_data
    -- Basic filter to ensure we only have valid records reaching Silver
    where customer_id is not null
)

select * from cleansed_data