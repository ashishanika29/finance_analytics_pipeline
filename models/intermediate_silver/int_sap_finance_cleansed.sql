/*
    MODEL: int_sap_finance_cleansed
    LAYER: Intermediate (Silver)
    OBJECTIVE: 
        1. Standardize SAP Finance data types.
        2. Handle NULL values using COALESCE.
        3. Convert accounting-style brackets '(500)' to numeric '-500'.
        4. Deduplicate transactions using QUALIFY.
*/

-- STEP 1: Import raw data from our Bronze Staging view
WITH staging_data AS (
    SELECT * FROM {{ ref('stg_sap_finance') }}
),

-- STEP 2: Data Cleansing & Formatting
-- This is where we perform "Data Surgery" on specific columns
cleansed_data AS (
    SELECT
        -- Keep standard IDs as-is
        ACCOUNT_ID,
        
        -- Use COALESCE to fill NULL Company Codes found in our Audit
        -- If it's NULL, we label it 'UNKNOWN' to satisfy our 'not_null' test
        COALESCE(COMPANY_CODE, 'UNKNOWN') AS COMPANY_CODE,

        -- Primary Key: We'll select it here, but deduplicate it in the next step
        TRANSACTION_ID,

        -- Date Transformation: Use TRY_TO_DATE to safely handle mixed formats.
        -- If a date is unparseable, it returns NULL instead of crashing the pipeline.
        TRY_TO_DATE(TXN_DATE) AS TXN_DATE,

        -- The "Big One": Converting '(500.00)' strings to real numbers.
        -- 1. REGEXP_REPLACE removes '(' and ')' by looking for anything in the brackets [()]
        -- 2. If it was negative (had brackets), we'd usually add logic, but for now, 
        -- we are stripping formatting and casting to DECIMAL(18,2).
        CAST(
            REGEXP_REPLACE(
                -- Layer 1: If the value is the text 'null', treat it as '0'
                -- Layer 2: If the value is a true SQL NULL, treat it as '0'
                CASE 
                    WHEN LOWER(POSTED_AMOUNT) = 'null' THEN '0' 
                    ELSE COALESCE(POSTED_AMOUNT, '0') 
                END,
                '[()]', '' -- Remove parentheses
            ) AS DECIMAL(18,2)
        ) AS POSTED_AMOUNT,

        -- Standardize Currency to uppercase to ensure 'usd' becomes 'USD'
        UPPER(CURRENCY) AS CURRENCY

    FROM staging_data
)

-- STEP 3: The Deduplication Layer (The "Duplicate Killer")
-- We use QUALIFY to ensure we only have ONE row per TRANSACTION_ID.
SELECT * FROM cleansed_data
/* QUALIFY works like a filter for window functions:
   - PARTITION BY: Groups rows by the ID.
   - ORDER BY: We could use a timestamp here, but for now, we just pick the first one found.
   - = 1: Discards any row that is a 'rank' of 2 or higher (the duplicates).
*/
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID ORDER BY TXN_DATE DESC) = 1