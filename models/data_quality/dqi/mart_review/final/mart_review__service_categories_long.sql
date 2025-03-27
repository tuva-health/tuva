{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with encounters as (
    select
        cast(c.year_month_int as {{ dbt.type_string() }}) as year_month
      , mc.person_id
      , service_category_1
      , service_category_2
      , count(distinct mc.encounter_id) as visits
    from {{ ref('mart_review__stg_medical_claim') }} as mc
    left outer join {{ ref('reference_data__calendar') }} as c on mc.claim_start_date = c.full_date
    inner join {{ ref ('core__encounter') }} as e
      on mc.encounter_id = e.encounter_id
    group by
        cast(c.year_month_int as {{ dbt.type_string() }})
      , mc.person_id
      , service_category_1
      , service_category_2
)


    select     
        pmpm.year_month
      , pmpm.data_source
      , pmpm.payer
      , pmpm.{{ quote_column('plan') }}
      , pmpm.service_category_1
      , pmpm.service_category_2
      , sum(total_paid) as paid_amt
      , sum(visits) as visits
    from {{ ref('financial_pmpm__patient_spend_with_service_categories') }} as pmpm
    left outer join encounters
      on pmpm.year_month = encounters.year_month
      and pmpm.person_id = encounters.person_id
      and pmpm.service_category_1 = encounters.service_category_1
      and pmpm.service_category_2 = encounters.service_category_2
    group by
        pmpm.year_month
      , pmpm.data_source
      , pmpm.payer
      , pmpm.{{ quote_column('plan') }}
      , pmpm.service_category_1
      , pmpm.service_category_2
