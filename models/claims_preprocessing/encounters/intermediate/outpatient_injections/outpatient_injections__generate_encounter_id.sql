WITH anchor AS 
(
    SELECT DISTINCT m.patient_id
        , m.start_date
        , m.claim_id
        , m.hcpcs_code
    FROM {{ ref('encounters__stg_medical_claim') }} m
    INNER JOIN {{ ref('outpatient_injections__anchor_events') }} u ON m.claim_id = u.claim_id
),
sorted_claims AS (
    SELECT patient_id
        , start_date
        , claim_id
        , hcpcs_code
        , LAG(start_date, 1) OVER (PARTITION BY patient_id, hcpcs_code ORDER BY start_date) AS prev_start_date
    FROM anchor
),
gaps_identified AS (
    SELECT patient_id
        , start_date
        , claim_id
        , hcpcs_code
        , prev_start_date
        , CASE 
            WHEN prev_start_date IS NULL OR DATEDIFF(MONTH, prev_start_date, start_date) > 6 THEN 1
            ELSE 0
          END AS new_encounter_start
    FROM sorted_claims
),
encounter_ids AS (
    SELECT patient_id
        , start_date
        , claim_id
        , hcpcs_code
        , SUM(new_encounter_start) OVER (PARTITION BY patient_id, hcpcs_code ORDER BY start_date) AS encounter_id
    FROM gaps_identified
)
SELECT patient_id
    , start_date
    , claim_id
    , hcpcs_code
    , encounter_id AS old_encounter_id
FROM encounter_ids
