{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with medical_claim as (
    select
        data_source
        , person_id
        , year_month
        , SUM(paid_amount) as paid_amount
    from {{ ref('mart_review__stg_medical_claim') }}
    group by data_source
    , person_id
    , year_month
)

,pharmacy_claim as (
    select
        data_source
        , person_id
        , year_month
        , SUM(paid_amount) as paid_amount
    from {{ ref('mart_review__stg_pharmacy_claim') }}
    group by data_source
    , person_id
    , year_month
)
, final as(
select
    mm.data_source
    , mm.year_month
    , SUM(case when mc.person_id is not null then 1 else 0 end) as members_with_medical_claims
    , SUM(case when pc.person_id is not null then 1 else 0 end) as members_with_pharmacy_claims
    , SUM(case when pc.person_id is not null then 1
             when mc.person_id is not null then 1 else 0 end) as members_with_claims
    , COUNT(*) as total_member_months
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__member_months') }} as mm
left outer join medical_claim as mc
    on mm.person_id = mc.person_id
    and mm.data_source = mc.data_source
    and mm.year_month = mc.year_month
left outer join pharmacy_claim as pc
    on mm.person_id = pc.person_id
    and mm.data_source = pc.data_source
    and mm.year_month = pc.year_month
group by mm.data_source, mm.year_month
)

select
    data_source
    , year_month
    , members_with_medical_claims
    , members_with_pharmacy_claims
    , members_with_claims
    , total_member_months
    , CAST(members_with_claims/ total_member_months as {{ dbt.type_numeric() }}) as percent_members_with_claims
    , CAST(members_with_medical_claims/ total_member_months  as {{ dbt.type_numeric() }}) as percent_members_with_medical_claims
    , CAST(members_with_pharmacy_claims/ total_member_months as {{ dbt.type_numeric() }})  as  percent_members_with_pharmacy_claims
from final
