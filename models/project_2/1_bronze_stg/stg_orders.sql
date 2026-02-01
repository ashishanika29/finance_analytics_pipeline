{{
    config(
        materialized='view',
        schema='SILVER_TRANFORMATION'
    )
}}

with source as (
    select * from {{ source('project_2_source', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_date,
        amount,
        status
    from source
)

select * from renamed