with source as (
    select * from {{ source('sap_raw_data', 'sap_finance_inbound') }} 
)
,
renamed as (
    select
        transaction_id,
        txn_date,
        company_code,
        account_id,
        posted_amount,
        currency
    from source
)

select * from renamed

