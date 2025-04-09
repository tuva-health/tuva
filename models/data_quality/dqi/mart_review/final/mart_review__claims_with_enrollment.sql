{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with enrollment_stage as(
select
    mc.data_source
    , mc.year_month
    , mc.payer
    , mc.{{ quote_column('plan') }}
    , SUM(case when mm.person_id is not null then 1 else 0 end) as claims_with_enrollment
    , COUNT(*) as claims
from {{ ref('mart_review__stg_medical_claim') }} as mc
left outer join {{ ref('core__member_months') }} as mm
    on mc.member_month_key = mm.member_month_key
group by mc.data_source
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
    , CAST(claims_with_enrollment / claims as {{ dbt.type_numeric() }} ) as percentage_claims_with_enrollment
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from enrollment_stage
