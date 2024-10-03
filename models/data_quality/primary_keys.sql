{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

WITH eligibility_pk as ( 
/*Eligibility PK test*/
SELECT 
    patient_id, 
    member_id, 
    enrollment_start_date, 
    enrollment_end_date, 
    COUNT(*) AS duplicate_count
FROM 
    {{ref('eligibility')}}
GROUP BY 
    patient_id, 
    member_id, 
    enrollment_start_date, 
    enrollment_end_date
HAVING 
    COUNT(*) > 1
) 

, Medical_Claim_PK AS (
/*Medical Claim PK test*/
SELECT 
    claim_id, 
    claim_line_number,
    COUNT(*) AS duplicate_count
FROM 
    {{ref('medical_claim')}}
GROUP BY 
    claim_id, 
    claim_line_number
HAVING 
    COUNT(*) > 1
) 

, Pharmacy_Claim_PK AS (
/*Pharmacy Claim PK test*/
SELECT 
    claim_id, 
    claim_line_number, 
    COUNT(*) AS duplicate_count
FROM 
    {{ref('pharmacy_claim')}}
GROUP BY 
    claim_id, 
    claim_line_number
HAVING 
    COUNT(*) > 1
) 

-- Final select to handle each case, including when no rows are returned
SELECT 
    'primary_key_duplicate' AS test_name,
    'eligibility' AS test_source,
    COALESCE(SUM(duplicate_count), 0) AS flagged_records,
FROM 
    eligibility_pk

UNION ALL 

SELECT 
    'primary_key_duplicate' AS test_name,
    'medical_claim' AS test_source,
    COALESCE(SUM(duplicate_count), 0) AS flagged_records,
FROM 
    Medical_Claim_PK

UNION ALL

SELECT 
    'primary_key_duplicate' AS test_name,
    'pharmacy_claim' AS test_source,
    COALESCE(SUM(duplicate_count), 0) AS flagged_records,
FROM 
    Pharmacy_Claim_PK
