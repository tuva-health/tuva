{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with enrollment_stage as(
SELECT
    mc.data_source,
    mc.year_month,
    mc.payer,
    mc.{{ quote_column('plan') }},
    SUM(CASE WHEN mm.person_id IS NOT NULL THEN 1 ELSE 0 END) AS claims_with_enrollment,
    COUNT(*) AS claims
FROM {{ ref('mart_review__stg_medical_claim') }} mc
LEFT JOIN {{ ref('core__member_months')}} mm
    ON mc.member_month_key = mm.member_month_key
GROUP BY mc.data_source
, mc.year_month
, mc.payer
, mc.{{ quote_column('plan') }}
)

select
    data_source
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , claims_with_enrollment
    , claims
    , cast(claims_with_enrollment / claims as {{ dbt.type_numeric()}} ) AS percentage_claims_with_enrollment
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from enrollment_stage