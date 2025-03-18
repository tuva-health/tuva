SELECT distinct
    year
    , month
    , year_month_int
    , first_day_of_month
FROM {{ ref('reference_data__calendar') }} cal
INNER JOIN {{ ref('core__medical_claim') }} mc on cal.full_date = coalesce(mc.claim_start_date,mc.claim_end_date)