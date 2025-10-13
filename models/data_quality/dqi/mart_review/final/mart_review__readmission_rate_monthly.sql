{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool,
     materialized = 'table'
   )
}}

-- Monthly readmission rate by data_source/payer/plan using calendar joins for portability
with stg as (
  select distinct data_source, payer, {{ quote_column('plan') }} as {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
), denom as (
  select i.data_source, s.payer, s.{{ quote_column('plan') }} as {{ quote_column('plan') }},
         cast(cal.year_month_int as {{ dbt.type_string() }}) as year_month,
         count(*) as index_admissions
  from {{ ref('mart_review__inpatient') }} i
  left join stg s
    on i.data_source = s.data_source and i.encounter_id = s.encounter_id
  left join {{ ref('reference_data__calendar') }} cal
    on i.encounter_end_date = cal.full_date
  group by 1,2,3,4
), num as (
  select coalesce(s.data_source,'') as data_source, s.payer, s.{{ quote_column('plan') }} as {{ quote_column('plan') }},
         cast(cal.year_month_int as {{ dbt.type_string() }}) as year_month,
         sum(case when r.unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
  from {{ ref('mart_review__readmissions') }} r
  left join stg s
    on r.encounter_id = s.encounter_id
  left join {{ ref('reference_data__calendar') }} cal
    on r.discharge_date = cal.full_date
  group by 1,2,3,4
)
select
  coalesce(d.data_source,n.data_source) as data_source,
  coalesce(d.payer,n.payer) as payer,
  coalesce(d.{{ quote_column('plan') }}, n.{{ quote_column('plan') }}) as {{ quote_column('plan') }},
  coalesce(d.year_month,n.year_month) as year_month,
  cast(n.readmissions as {{ dbt.type_numeric() }}) / nullif(d.index_admissions, 0) as readmission_rate,
  d.index_admissions,
  n.readmissions,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from denom d
full outer join num n using (data_source,payer,{{ quote_column('plan') }},year_month)
