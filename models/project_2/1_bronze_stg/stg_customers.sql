with source as (
    select * from {{ source('project_2_source', 'customers') }}
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        signup_date
    from source
)

select * from renamed