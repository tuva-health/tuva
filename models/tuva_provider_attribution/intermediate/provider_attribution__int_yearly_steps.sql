{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

-- Yearly attribution steps (ACO-agnostic): Step 1 (PCP/NPP), Step 2 (Specialist), Step 3 (expanded window PCP/NPP)

with person_years as (
  select * from {{ ref('provider_attribution__int_person_years') }}
)

, claims as (
  select * from {{ ref('provider_attribution__int_primary_care_claims') }}
)

, step1 as (
  -- 12-month window: Jan..Dec of performance_year, PCP/NPP
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 1 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from person_years as py
  inner join claims as c
    on py.person_id = c.person_id
   and c.claim_year = py.performance_year
   and c.provider_bucket in ('pcp', 'npp')
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step1_benes as (
  select distinct person_id, performance_year from step1
)

, step2 as (
  -- 12-month window: Jan..Dec of performance_year, Specialists only; only for benes not in step1
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 2 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from person_years as py
  inner join claims as c
    on py.person_id = c.person_id
   and c.claim_year = py.performance_year
   and c.provider_bucket = 'specialist'
  left outer join step1_benes as s1
    on s1.person_id = py.person_id and s1.performance_year = py.performance_year
  where s1.person_id is null
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step2_benes as (
  select distinct person_id, performance_year from step2
)

, step3 as (
  -- 24-month expanded: Jan of Y-1 .. Dec of Y, PCP/NPP only; only for benes not in step1/step2
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 3 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from person_years as py
  inner join claims as c
    on py.person_id = c.person_id
   and c.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                  and (py.performance_year * 100 + 12)
   and c.provider_bucket in ('pcp', 'npp')
  left outer join step1_benes as s1
    on s1.person_id = py.person_id and s1.performance_year = py.performance_year
  left outer join step2_benes as s2
    on s2.person_id = py.person_id and s2.performance_year = py.performance_year
  where s1.person_id is null
    and s2.person_id is null
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

select * from step1
union all
select * from step2
union all
select * from step3
