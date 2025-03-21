{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with medical as (
  select
      'medical_claim' as table_name
    , count(*) as record_count
  from {{ ref('core__medical_claim') }}
)

, pharmacy as (
  select
      'pharmacy_claim' as table_name
    , count(*) as record_count
  from {{ ref('core__pharmacy_claim') }}
)

, eligibility as (
  select
      'eligibility' as table_name
    , count(*) as record_count
  from {{ ref('core__eligibility') }}
)

, member_months as (
  select
      'member_months' as table_name
    , count(*) as record_count
  from {{ ref('core__member_months') }}
)

, patient as (
  select
      'patient' as table_name
    , count(*) as record_count
  from {{ ref('core__patient') }}
)

, encounter as (
  select
      'encounter' as table_name
    , count(*) as record_count
  from {{ ref('core__encounter') }}
)

, condition as (
  select
      'condition' as table_name
    , count(*) as record_count
  from {{ ref('core__condition') }}
)

, procedure_cte as (
  select
      'procedure' as table_name
    , count(*) as record_count
  from {{ ref('core__procedure') }}
)

,final as (
select * from medical
union all
select * from pharmacy
union all
select * from eligibility
union all
select * from member_months
union all
select * from patient
union all
select * from encounter
union all
select * from condition
union all
select * from procedure_cte
)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final