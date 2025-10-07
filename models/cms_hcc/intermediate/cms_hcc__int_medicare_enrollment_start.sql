{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

/*
    Source: CMS Medicare Managed Care Manual, Chapter 7, p.11.
    "Operationally, CMS identifies new enrollees as those beneficiaries with less than
    12 months of Medicare Part B entitlement during the data collection year."
    We therefore anchor "new vs continuing" to Medicare Part B entitlement start.
*/

with actual as (

    select
          person_id
        , min(medicare_part_b_enrollment_start_date) as actual_start
    from {{ ref('cms_hcc__stg_core__eligibility') }}
    group by person_id

)

/*
    Infer Part B start from Medicare claims when actual start is unavailable.
    Simplified per guidance:
      - Part B claim types: claim_type = 'professional' OR (claim_type = 'institutional' and service_category_1 <> 'inpatient')
      - Medicare detection via eligibility payer_type in ('medicare','medicare advantage')
*/
, medicare_claims as (

    select mc.person_id, mc.claim_type, mc.service_category_1, mc.claim_end_date
    from {{ ref('core__medical_claim') }} as mc
    inner join {{ ref('core__stg_claims_eligibility') }} as e
        on mc.person_id = e.person_id
        and lower(e.payer_type) in ('medicare','medicare advantage')
        and mc.claim_end_date between e.enrollment_start_date and e.enrollment_end_date
)

, part_b_candidate_claims as (

    select person_id, claim_end_date
    from medicare_claims
    where (
        claim_type = 'professional'
        or (claim_type = 'institutional' and lower(coalesce(service_category_1,'')) <> 'inpatient')
    )
)

, inferred as (

    select
          person_id
        , min(claim_end_date) as inferred_start
    from part_b_candidate_claims
    group by person_id

)

, combined as (

    select
          coalesce(actual.person_id, inferred.person_id) as person_id
        , actual.actual_start
        , inferred.inferred_start
    from actual
    full outer join inferred
        on actual.person_id = inferred.person_id
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(coalesce(actual_start, inferred_start) as date) as final_start
        , cast(case
                when actual_start is not null then 'actual'
                when inferred_start is not null then 'inferred_from_claims'
                else null
              end as {{ dbt.type_string() }}) as start_source
    from combined

)

select
      person_id
    , final_start
    , start_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
