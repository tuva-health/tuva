{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_5', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'warn'
   )
}}

select
    payer
    , {{ quote_column('plan') }}
    , data_source
    , count(year_month) as member_months
    , avg(total_paid) as avg_paid_pmpm
from {{ ref('financial_pmpm__pmpm_prep') }}
group by
    payer
    , data_source
    , {{ quote_column('plan') }}
having avg(total_paid) > 10000
and count(year_month) >= 100
