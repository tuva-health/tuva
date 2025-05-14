SELECT distinct
    year
FROM {{ ref('reference_data__calendar') }} cal
INNER JOIN {{ ref('core__medical_claim') }} mc on cal.full_date = coalesce(mc.claim_start_date,mc.claim_end_date)