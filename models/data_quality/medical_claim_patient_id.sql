WITH medical_claim_patient_id_null AS (
SELECT 
    claim_id, 
    claim_line_number, 
    CASE WHEN patient_id is null then 1 else 0 end as patient_id_null
FROM 
    {{ref('core__medical_claim')}}
) 

, medical_claim_multiple_patient_ids AS (
SELECT 
    claim_id, 
    COUNT(DISTINCT patient_id) AS number_of_patient_ids
FROM 
    {{ref('core__medical_claim')}}
GROUP BY claim_id
HAVING 
    COUNT(DISTINCT patient_id) > 1
) 

, orphaned_medical_claims AS (
SELECT  
    medical_claim.claim_id, 
    medical_claim.claim_line_number,  
    medical_claim.member_id AS claim_member_ID, 
    eligibility.member_ID AS eligibility_member_ID, 
    CASE WHEN eligibility.member_id IS NULL THEN 1 ELSE 0 END AS orphaned_claim
FROM  
    {{ref('core__medical_claim')}}
LEFT JOIN 
eligibility 
ON medical_claim.member_id = eligibility.member_id --need to join on patient_ID as well? 
) 

--Summary Output
SELECT  
'multiple_patient_id' AS test_name,
'medical_claim' AS test_source, 
COALESCE(COUNT(*),0) AS flagged_records
FROM 
medical_claim_multiple_patient_ids

UNION 

SELECT 
'missing_patient_id' AS test_name,
'medical_claim' AS test_source, 
COALESCE(SUM(patient_id_null),0) AS flagged_records
FROM 
medical_claim_patient_id_null

UNION 

SELECT 
'orphaned_claim' AS test_name,
'medical_claim' AS test_source, 
COALESCE(SUM(orphaned_claim),0) AS flagged_records
FROM 
orphaned_medical_claims

