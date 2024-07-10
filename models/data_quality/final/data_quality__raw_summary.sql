{{ config(
    enabled = var('claims_enabled', False)
) }}

select 
    'INPUT_LAYER' AS SOURCE
    ,'ELIGIBILITY' as TABLE_NAME
    ,count(*) as ROW_COUNT
    ,count(distinct patient_id) as UNIQUE_CHECK
    ,'Unique Patient Count' as UNIQUE_CHECK_DESC
    ,2 AS TABLE_ORDER
from {{ ref('eligibility')}}

union

select 
    'RAW_DATA' AS SOURCE
    ,'ELIGIBILITY' as TABLE_NAME
    ,NULL as ROW_COUNT
    ,NULL as UNIQUE_CHECK
    ,'Unique Patient Count' as UNIQUE_CHECK_DESC
    ,1 AS TABLE_ORDER
from {{ ref('eligibility')}}

union

select 
    'INPUT_LAYER' AS SOURCE
    ,'MEDICAL_CLAIM' as TABLE_NAME
    ,count(*) as ROW_COUNT
    ,count(distinct claim_id) as UNIQUE_CHECK
    ,'Unique Claim Count' as UNIQUE_CHECK_DESC
    ,4 AS TABLE_ORDER
from {{ ref('medical_claim')}}

union

select 
    'RAW_DATA' AS SOURCE
    ,'MEDICAL_CLAIM' as TABLE_NAME
    ,NULL as ROW_COUNT
    ,NULL as UNIQUE_CHECK
    ,'Unique Claim Count' as UNIQUE_CHECK_DESC
    ,3 AS TABLE_ORDER
from {{ ref('medical_claim')}}

union

select 
    'INPUT_LAYER' AS SOURCE
    ,'PHARMACY_CLAIM' as TABLE_NAME
    ,count(*) as ROW_COUNT
    ,count(distinct claim_id) as UNIQUE_CHECK
    ,'Unique Claim Count' as UNIQUE_CHECK_DESC
    ,6 AS TABLE_ORDER
from {{ ref('pharmacy_claim')}}

union

select 
    'RAW_DATA' AS SOURCE
    ,'PHARMACY_CLAIM' as TABLE_NAME
    ,NULL as ROW_COUNT
    ,NULL as UNIQUE_CHECK
    ,'Unique Claim Count' as UNIQUE_CHECK_DESC
    ,5 AS TABLE_ORDER
from {{ ref('pharmacy_claim')}}