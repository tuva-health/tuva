{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
     materialized = 'table'
   )
}}

-- Unified export: unions pre-aggregated mart_review models into a long format.
-- Columns: data_source, payer, plan, year_month, metric, dim1_name, dim1_value, dim1_label, dim2_name, dim2_value, dim2_label, value, tuva_last_run

-- Age distribution
select
  data_source,
  payer,
  {{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'age_distribution' as metric,
  'age_bucket' as dim1_name,
  age_bucket as dim1_value,
  age_bucket as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name,
  cast(null as {{ dbt.type_string() }}) as dim2_value,
  cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(count_members as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__age_distribution') }}

union all

-- Gender distribution
select
  data_source,
  payer,
  {{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'gender_distribution' as metric,
  'gender' as dim1_name,
  gender as dim1_value,
  gender as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name,
  cast(null as {{ dbt.type_string() }}) as dim2_value,
  cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(count_members as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__gender_distribution') }}

union all

-- PMPM by month (unpivot columns to metrics)
select data_source, payer, {{ quote_column('plan') }}, cast(year_month as {{ dbt.type_string() }}) as year_month,
  'pmpm_total' as metric, cast(null as {{ dbt.type_string() }}) as dim1_name, cast(null as {{ dbt.type_string() }}) as dim1_value, cast(null as {{ dbt.type_string() }}) as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(total_paid as {{ dbt.type_numeric() }}) as value, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__pmpm') }}

union all

select data_source, payer, {{ quote_column('plan') }}, cast(year_month as {{ dbt.type_string() }}) as year_month,
  'pmpm_medical' as metric, cast(null as {{ dbt.type_string() }}) as dim1_name, cast(null as {{ dbt.type_string() }}) as dim1_value, cast(null as {{ dbt.type_string() }}) as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(medical_paid as {{ dbt.type_numeric() }}) as value, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__pmpm') }}

union all

select data_source, payer, {{ quote_column('plan') }}, cast(year_month as {{ dbt.type_string() }}) as year_month,
  'pmpm_pharmacy' as metric, cast(null as {{ dbt.type_string() }}) as dim1_name, cast(null as {{ dbt.type_string() }}) as dim1_value, cast(null as {{ dbt.type_string() }}) as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(pharmacy_paid as {{ dbt.type_numeric() }}) as value, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__pmpm') }}

union all

-- Members with claims % by month
select data_source, cast(year_month as {{ dbt.type_string() }}) as year_month,
  cast(null as {{ dbt.type_string() }}) as payer, cast(null as {{ dbt.type_string() }}) as {{ quote_column('plan') }},
  'members_with_claims_pct' as metric,
  cast(null as {{ dbt.type_string() }}) as dim1_name, cast(null as {{ dbt.type_string() }}) as dim1_value, cast(null as {{ dbt.type_string() }}) as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(percent_members_with_claims as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__members_with_claims') }}

union all

-- Claims with enrollment % by month
select data_source, cast(year_month as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'claims_with_enrollment_pct' as metric,
  cast(null as {{ dbt.type_string() }}) as dim1_name, cast(null as {{ dbt.type_string() }}) as dim1_value, cast(null as {{ dbt.type_string() }}) as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(percentage_claims_with_enrollment as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__claims_with_enrollment') }}

union all

-- Chronic condition frequency
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'condition_count' as metric,
  'condition' as dim1_name, condition as dim1_value, condition as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(count_members as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__condition_counts') }}

union all

-- Inpatient LOS
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'inpatient_los' as metric,
  'los_groups' as dim1_name, los_groups as dim1_value, los_groups as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(encounters as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__inpatient_los_distribution') }}

union all

-- DRG counts
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'drg_count' as metric,
  'drg_code' as dim1_name, cast(drg_code as {{ dbt.type_string() }}) as dim1_value, drgwithdescription as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(encounters as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__drg_counts') }}

union all

-- ED avoidable
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'ed_avoidable' as metric,
  'avoidable_category' as dim1_name, avoidable_category as dim1_value, avoidable_category as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(encounters as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__ed_avoidable_counts') }}

union all

-- Service categories paid
select data_source, payer, {{ quote_column('plan') }}, cast(year_month as {{ dbt.type_string() }}) as year_month,
  'svc_paid_amt' as metric,
  'service_category_1' as dim1_name, service_category_1 as dim1_value, service_category_1 as dim1_label,
  'service_category_2' as dim2_name, service_category_2 as dim2_value, service_category_2 as dim2_label,
  cast(paid_amt as {{ dbt.type_numeric() }}) as value, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__service_categories_long') }}

union all

-- Service categories visits
select data_source, payer, {{ quote_column('plan') }}, cast(year_month as {{ dbt.type_string() }}) as year_month,
  'svc_visits' as metric,
  'service_category_1' as dim1_name, service_category_1 as dim1_value, service_category_1 as dim1_label,
  'service_category_2' as dim2_name, service_category_2 as dim2_value, service_category_2 as dim2_label,
  cast(visits as {{ dbt.type_numeric() }}) as value, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__service_categories_long') }}

union all

-- Encounters by group: count
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'encounter_count_by_group' as metric,
  'encounter_group' as dim1_name, encounter_group as dim1_value, encounter_group as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(encounters as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__encounters_by_group') }}

union all

-- Encounters by group: paid
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'encounter_paid_by_group' as metric,
  'encounter_group' as dim1_name, encounter_group as dim1_value, encounter_group as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(paid_amount as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__encounters_by_group') }}

union all

-- Encounters by type: count
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'encounter_count_by_type' as metric,
  'encounter_type' as dim1_name, encounter_type as dim1_value, encounter_type as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(encounters as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__encounters_by_type') }}

union all

-- Encounters by type: paid
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'encounter_paid_by_type' as metric,
  'encounter_type' as dim1_name, encounter_type as dim1_value, encounter_type as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(paid_amount as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__encounters_by_type') }}

union all

-- HCC counts
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'hcc_count' as metric,
  'hcc_code' as dim1_name, hcc_code as dim1_value, hcc_code as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name, cast(null as {{ dbt.type_string() }}) as dim2_value, cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(count_members as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__hcc_counts') }}

union all

-- Pharmacy summary: spend
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  'pharm_spend' as metric,
  'theraclass' as dim1_name, theraclass as dim1_value, theraclass as dim1_label,
  'ndc_code' as dim2_name, ndc_code as dim2_value, ndc_description as dim2_label,
  cast(spend as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from {{ ref('mart_review__pharmacy_summary') }}

union all

-- Pharmacy summary: averages
select data_source, cast(null as {{ dbt.type_string() }}) as year_month,
  payer, {{ quote_column('plan') }},
  metric,
  'theraclass' as dim1_name, theraclass as dim1_value, theraclass as dim1_label,
  'ndc_code' as dim2_name, ndc_code as dim2_value, ndc_description as dim2_label,
  cast(value as {{ dbt.type_numeric() }}) as value,
  tuva_last_run
from (
  select data_source, payer, {{ quote_column('plan') }}, theraclass, ndc_code, ndc_description, 'pharm_avg_days_supply' as metric, avg_days_supply as value, tuva_last_run from {{ ref('mart_review__pharmacy_summary') }}
  union all
  select data_source, payer, {{ quote_column('plan') }}, theraclass, ndc_code, ndc_description, 'pharm_avg_quantity' as metric, avg_quantity as value, tuva_last_run from {{ ref('mart_review__pharmacy_summary') }}
  union all
  select data_source, payer, {{ quote_column('plan') }}, theraclass, ndc_code, ndc_description, 'pharm_avg_refills' as metric, avg_refills as value, tuva_last_run from {{ ref('mart_review__pharmacy_summary') }}
) x

union all

-- Members by state
select
  p.data_source,
  m.payer,
  m.{{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'state_members' as metric,
  'state' as dim1_name,
  p.state as dim1_value,
  p.state as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name,
  cast(null as {{ dbt.type_string() }}) as dim2_value,
  cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(count(distinct p.person_id) as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__patient') }} p
left join {{ ref('mart_review__member_months') }} m
  on p.person_id = m.person_id and p.data_source = m.data_source
where p.state is not null
group by p.data_source, m.payer, m.{{ quote_column('plan') }}, p.state

union all

-- ED avoidable spend by category
select
  ed.data_source,
  epp.payer,
  epp.{{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'ed_avoidable_spend' as metric,
  'avoidable_category' as dim1_name,
  ed.avoidable_category as dim1_value,
  ed.avoidable_category as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name,
  cast(null as {{ dbt.type_string() }}) as dim2_value,
  cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(sum(coalesce(ed.paid_amount,0)) as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__ed_classification') }} ed
left join (
  select distinct data_source, payer, {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
) epp on ed.data_source = epp.data_source and ed.encounter_id = epp.encounter_id
group by ed.data_source, epp.payer, epp.{{ quote_column('plan') }}, ed.avoidable_category

union all

-- ED spend by facility
select
  ed.data_source,
  epp2.payer,
  epp2.{{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'ed_facility_spend' as metric,
  'facility_id' as dim1_name,
  ed.facility_id as dim1_value,
  ed.facility_id as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name,
  cast(null as {{ dbt.type_string() }}) as dim2_value,
  cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(sum(coalesce(ed.paid_amount,0)) as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__ed_classification') }} ed
left join (
  select distinct data_source, payer, {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
) epp2 on ed.data_source = epp2.data_source and ed.encounter_id = epp2.encounter_id
group by ed.data_source, epp2.payer, epp2.{{ quote_column('plan') }}, ed.facility_id

union all

-- Discharge location by facility (inpatient)
select
  i.data_source,
  epp3.payer,
  epp3.{{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'discharge_by_facility' as metric,
  'facility_id' as dim1_name,
  cast(i.facility_id as {{ dbt.type_string() }}) as dim1_value,
  cast(i.facility_id as {{ dbt.type_string() }}) as dim1_label,
  'discharge_location' as dim2_name,
  i.discharge_disposition_description as dim2_value,
  i.discharge_disposition_description as dim2_label,
  cast(count(distinct i.encounter_id) as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__inpatient') }} i
left join (
  select distinct data_source, payer, {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
) epp3 on i.data_source = epp3.data_source and i.encounter_id = epp3.encounter_id
where i.discharge_disposition_description is not null
group by i.data_source, epp3.payer, epp3.{{ quote_column('plan') }}, i.facility_id, i.discharge_disposition_description

union all

-- Readmission rate (unplanned 30-day)
select
  coalesce(epp4.data_source,'') as data_source,
  epp4.payer,
  epp4.{{ quote_column('plan') }},
  cast(null as {{ dbt.type_string() }}) as year_month,
  'readmission_rate' as metric,
  cast(null as {{ dbt.type_string() }}) as dim1_name,
  cast(null as {{ dbt.type_string() }}) as dim1_value,
  cast(null as {{ dbt.type_string() }}) as dim1_label,
  cast(null as {{ dbt.type_string() }}) as dim2_name,
  cast(null as {{ dbt.type_string() }}) as dim2_value,
  cast(null as {{ dbt.type_string() }}) as dim2_label,
  cast(avg(case when r.unplanned_readmit_30_flag = 1 then 1.0 else 0.0 end) as {{ dbt.type_numeric() }}) as value,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__readmissions') }} r
left join (
  select distinct data_source, payer, {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
) epp4 on r.encounter_id = epp4.encounter_id
group by coalesce(epp4.data_source,''), epp4.payer, epp4.{{ quote_column('plan') }}
