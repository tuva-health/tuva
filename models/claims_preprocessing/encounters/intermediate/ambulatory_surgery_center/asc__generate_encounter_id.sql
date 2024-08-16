WITH anchor AS (
    SELECT DISTINCT
        m.patient_id,
        m.start_date,
        m.end_date,
        m.claim_id
    FROM {{ ref('encounters__stg_medical_claim') }} m
    INNER JOIN {{ ref('asc__anchor_events') }} u ON m.claim_id = u.claim_id
),
sorted_data AS (
    SELECT
        patient_id,
        start_date,
        end_date,
        claim_id,
        LAG(end_date) OVER (PARTITION BY patient_id ORDER BY start_date, end_date) AS previous_end_date
    FROM anchor
),
grouped_data AS (
    SELECT
        patient_id,
        start_date,
        end_date,
        claim_id,
        CASE
            WHEN previous_end_date IS NULL OR previous_end_date < start_date THEN 1
            ELSE 0
        END AS is_new_group
    FROM sorted_data
),
encounters AS (
    SELECT
        patient_id,
        start_date,
        end_date,
        claim_id,
        SUM(is_new_group) OVER (ORDER BY patient_id, start_date) AS old_encounter_id
    FROM grouped_data
)

SELECT
    patient_id,
    start_date,
    end_date,
    claim_id,
    old_encounter_id
FROM encounters

