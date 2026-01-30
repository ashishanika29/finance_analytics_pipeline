/*
    MODEL: marts_sap_finance
    LAYER: Gold (Marts)
    OBJECTIVE: 
        1. Final presentation layer for Power BI.
        2. Renames technical column names to Business-Friendly names.
        3. Points to the physical Silver table for performance.
*/

-- STEP 1: Import the cleaned data from Silver
-- Use the 'ref' function to create the lineage link

{{ config(schema='GOLD_MART_FINANCE') }}

WITH silver_data AS (
    SELECT * FROM {{ ref('int_sap_finance_cleansed') }}
),

-- STEP 2: Final Mapping
-- We rename technical names (Dev-speak) to professional names (User-speak)
final_presentation AS (
    SELECT
        TRANSACTION_ID,
        
        -- Rename TXN_DATE to something a Finance Manager understands
        TXN_DATE AS TRANSACTION_DATE,
        
        -- Rename COMPANY_CODE to ENTITY_NAME as per YAML
        COMPANY_CODE AS ENTITY_NAME,
        
        -- Keep ACCOUNT_ID as is
        ACCOUNT_ID,
        
        -- Rename POSTED_AMOUNT to TOTAL_AMOUNT as per YAML
        POSTED_AMOUNT AS TOTAL_AMOUNT,
        
        CURRENCY
    FROM silver_data
)

-- STEP 3: Final Output
-- Since this is a View, Snowflake will run this 'recipe' every time Power BI refreshes
SELECT * FROM final_presentation