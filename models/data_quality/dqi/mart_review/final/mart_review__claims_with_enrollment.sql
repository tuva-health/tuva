{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with enrollment_stage as(
SELECT
    mc.data_source,
    mc.year_month,
    SUM(CASE WHEN mm.person_id IS NOT NULL THEN 1 ELSE 0 END) AS claims_with_enrollment,
    COUNT(*) AS claims
FROM {{ ref('mart_review__stg_medical_claim') }} mc
LEFT JOIN {{ ref('core__member_months')}} mm
    ON mc.person_id = mm.person_id
    AND mc.data_source = mm.data_source
    AND mc.year_month = mm.year_month
GROUP BY mc.data_source
, mc.year_month
)

select
    data_source
    , year_month
    , claims_with_enrollment
    , claims
    , cast(claims_with_enrollment / claims as {{ dbt.type_numeric()}} ) AS percentage_claims_with_enrollment
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from enrollment_stage