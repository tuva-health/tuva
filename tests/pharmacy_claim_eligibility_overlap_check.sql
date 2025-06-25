{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_2', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'warn'
   )
}}

with eligibility as (
    select distinct
        person_id
        , data_source
    from {{ ref('input_layer__eligibility') }}
)

, pharmacy_claims as (
    select distinct
        person_id
        , data_source
    from {{ ref('input_layer__pharmacy_claim') }}
)

, pc_records_check as (
    select
        data_source
        , count(*) as n_rows
    from pharmacy_claims
    group by data_source
)

, elig_records_check as (
    select
        data_source
        , count(*) as n_rows
    from eligibility
    group by data_source
)

, overlap_check as (
    select
        p.data_source
        , count(*) as n_rows
    from pharmacy_claims as p
    inner join eligibility as e
    on p.person_id = e.person_id
    and p.data_source = e.data_source
    group by p.data_source
)

, final as (
    select
        oc.data_source
        , oc.n_rows as n_overlapping_records
        , coalesce(pc.n_rows, 0) = 0 as is_pc_empty
        , coalesce(ec.n_rows, 0) = 0 as is_elig_empty
    from overlap_check as oc
    left outer join pc_records_check as pc
    on oc.data_source = pc.data_source
    left outer join elig_records_check as ec
    on oc.data_source = ec.data_source
)

select
    data_source
    , n_overlapping_records
from final
where not (is_pc_empty or is_elig_empty)
and n_overlapping_records = 0
