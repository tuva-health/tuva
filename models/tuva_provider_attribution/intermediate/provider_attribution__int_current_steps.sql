{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

-- Current rolling 12-month attribution steps using data-driven as_of_date

with claim_bounds as (
  select max(claim_end_date) as max_claim_end_date
  from {{ ref('provider_attribution__int_primary_care_claims') }}
)

{% set override_as_of_date = var('provider_attribution_as_of_date', none) %}

, params as (
  select
    {% if override_as_of_date %}
      cast('{{ override_as_of_date }}' as date) as as_of_date
    {% else %}
      case
        when max_claim_end_date is not null
          and max_claim_end_date <= cast({{ dbt.current_timestamp() }} as date)
          then max_claim_end_date
        else cast({{ dbt.current_timestamp() }} as date)
      end as as_of_date
    {% endif %}
  from claim_bounds
)

, months as (
  -- Build the last 12 calendar months (YYYYMM) ending at as_of_date
  select distinct
      c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }} as c
  cross join params as p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, claims as (
  select
      c.person_id
    , c.provider_id
    , c.provider_bucket
    , c.prov_specialty
    , c.encounter_id
    , c.claim_id
    , c.claim_year_month
    , c.claim_year_month_int
    , c.allowed_amount
  from {{ ref('provider_attribution__int_primary_care_claims') }} as c
  inner join months as m
    on c.claim_year_month_int = m.year_month_int
  cross join params as p
  where c.claim_end_date <= p.as_of_date
)

, step1 as (
  select
      c.person_id
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 1 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from claims as c
  where c.provider_bucket in ('pcp', 'npp')
  group by c.person_id, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step1_benes as (
  select distinct person_id from step1
)

, step2 as (
  select
      c.person_id
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 2 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from claims as c
  left outer join step1_benes as s1 on s1.person_id = c.person_id
  where s1.person_id is null and c.provider_bucket = 'specialist'
  group by c.person_id, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

select * from step1
union all
select * from step2
