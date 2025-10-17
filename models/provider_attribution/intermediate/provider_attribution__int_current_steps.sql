{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

-- Current rolling 12-month attribution steps using var('tuva_last_run') as as_of_date

with params as (
  select 
    cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as as_of_date
)

, months as (
  -- Build the last 12 calendar months (YYYYMM) ending at as_of_date
  select distinct 
      {{ concat_custom(["c.year", dbt.right(concat_custom(["'0'", "c.month"]), 2)]) }} as year_month
  from {{ ref('reference_data__calendar') }} c
  cross join params p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, claims as (
  select 
      c.person_id
    , c.provider_id
    , c.bucket
    , c.prov_specialty
    , c.claim_id
    , c.claim_year_month
    , c.allowed_amount
  from {{ ref('provider_attribution__int_primary_care_claims') }} c
  inner join months m
    on c.claim_year_month = m.year_month
)

, step1 as (
  select 
      c.person_id
    , c.provider_id
    , coalesce(c.bucket,'unknown') as bucket
    , c.prov_specialty
    , 1 as step
    , sum(c.allowed_amount) as allowed_charges
    , count(distinct c.claim_id) as visits
  from claims c
  where c.bucket in ('pcp','npp')
  group by c.person_id, c.provider_id, coalesce(c.bucket,'unknown'), c.prov_specialty
)

, step1_benes as (
  select distinct person_id from step1
)

, step2 as (
  select 
      c.person_id
    , c.provider_id
    , coalesce(c.bucket,'unknown') as bucket
    , c.prov_specialty
    , 2 as step
    , sum(c.allowed_amount) as allowed_charges
    , count(distinct c.claim_id) as visits
  from claims c
  left join step1_benes s1 on s1.person_id = c.person_id
  where s1.person_id is null and c.bucket = 'specialist'
  group by c.person_id, c.provider_id, coalesce(c.bucket,'unknown'), c.prov_specialty
)

select * from step1
union all
select * from step2
