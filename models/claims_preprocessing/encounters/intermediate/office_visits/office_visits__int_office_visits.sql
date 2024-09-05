with anchor as (
    SELECT DISTINCT
        mc.patient_id,
        mc.start_date,
        mc.claim_id,
        mc.claim_line_number,
        mc.service_category_1,
        mc.service_category_2,
        mc.service_category_3
    FROM {{ ref('encounters__stg_medical_claim') }} mc
    INNER JOIN {{ ref('encounters__stg_professional') }} p 
        ON mc.claim_id = p.claim_id 
        AND mc.claim_line_number = p.claim_line_number
    WHERE mc.service_category_1 = 'Office-Based'
)

select patient_id
,start_date
,claim_id
,claim_line_number
,service_category_1
,service_category_2
,service_category_3
,dense_rank() over (order by patient_id, start_date) as old_encounter_id
from anchor