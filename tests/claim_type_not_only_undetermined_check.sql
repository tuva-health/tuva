{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_2', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'warn'
   )
}}

with claim_type_cte as (
    select
        data_source
        , min(claim_type = 'undetermined') as has_only_undetermined_claim_types
        , count(*) as count_records
    from {{ ref('input_layer__medical_claim') }}
    group by data_source
)

select
    *
from claim_type_cte
where has_only_undetermined_claim_types
