{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with tuva_chronic_condition_long as (
  select
      'tuva_chronic_condition_long' as table_name
    , count(*) as record_count
  from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
)

, cms_hcc_patient_risk_factors as (
  select
      'cms_hcc__patient_risk_factors' as table_name
    , count(*) as record_count
  from {{ ref('cms_hcc__patient_risk_factors') }}
)

, service_category_grouper as (
  select
      'service_category__service_category_grouper' as table_name
    , count(*) as record_count
  from {{ ref('service_category__service_category_grouper') }}
)

, financial_pmpm_payer as (
  select
      'financial_pmpm__pmpm_payer' as table_name
    , count(*) as record_count
  from {{ ref('financial_pmpm__pmpm_payer') }}
)

, readmission_summary as (
  select
      'readmissions__readmission_summary' as table_name
    , count(*) as record_count
  from {{ ref('readmissions__readmission_summary') }}
)

, quality_measures_summary_long as (
  select
      'quality_measures__summary_long' as table_name
    , count(*) as record_count
  from {{ ref('quality_measures__summary_long') }}
)

, acute_inpatient_visits as (
  select
      'acute_inpatient_visits' as table_name
    , count(*) as record_count
  from {{ ref('core__encounter') }}
  where encounter_type = 'acute inpatient'
)

, ed_visits as (
  select
      'ed_visits' as table_name
    , count(*) as record_count
  from {{ ref('core__encounter') }}
  where encounter_type = 'emergency department'
)

,final as (
select * from tuva_chronic_condition_long
union all
select * from cms_hcc_patient_risk_factors
union all
select * from service_category_grouper
union all
select * from financial_pmpm_payer
union all
select * from readmission_summary
union all
select * from quality_measures_summary_long
union all
select * from acute_inpatient_visits
union all
select * from ed_visits
)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final