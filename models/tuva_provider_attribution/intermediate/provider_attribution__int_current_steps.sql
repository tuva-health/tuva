{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

-- Current rolling attribution steps using a data-driven as_of_date.
-- Steps:
--   1 = PCP/NPP rendering primary-care HCPCS inside the most recent 12 months.
--   2 = Specialist rendering primary-care HCPCS (12 months) for beneficiaries not assigned in step 1.
--   3 = PCP/NPP rendering primary-care HCPCS across the expanded 24-month window.
--   4 = Primary-care HCPCS fallback across 24 months regardless of provider classification.
--   5 = Any rendering NPI across 24 months (captures claims outside the HCPCS list or with null HCPCS/NPIs).

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

, months_12 as (
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

, months_24 as (
  -- Build the last 24 calendar months (YYYYMM) ending at as_of_date
  select distinct
      c.year_month_int
    , c.first_day_of_month
    , c.last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }} as c
  cross join params as p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-23, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, claims_12 as (
  select
      c.person_id
    , c.provider_id
    , c.provider_bucket
    , c.prov_specialty
    , c.encounter_id
    , c.claim_id
    , c.claim_year_month
    , c.claim_year_month_int
    , c.claim_end_date
    , c.allowed_amount
  from {{ ref('provider_attribution__int_primary_care_claims') }} as c
  inner join months_12 as m
    on c.claim_year_month_int = m.year_month_int
  cross join params as p
  where c.claim_end_date <= p.as_of_date
)

, claims_24 as (
  select
      c.person_id
    , c.provider_id
    , c.provider_bucket
    , c.prov_specialty
    , c.encounter_id
    , c.claim_id
    , c.claim_year_month
    , c.claim_year_month_int
    , c.claim_end_date
    , c.allowed_amount
  from {{ ref('provider_attribution__int_primary_care_claims') }} as c
  inner join months_24 as m
    on c.claim_year_month_int = m.year_month_int
  cross join params as p
  where c.claim_end_date <= p.as_of_date
)

, all_claim_month as (
  select
      mc.person_id
    , mc.claim_id
    , mc.claim_line_number
    , cast(cm.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , mc.claim_start_date
    , mc.claim_end_date
    , cal.year_month_int as claim_year_month_int
    , cast(cal.year_month_int as {{ dbt.type_string() }}) as claim_year_month
    , coalesce(nullif(mc.allowed_amount, 0), mc.paid_amount, 0) as allowed_amount
    , cast(mc.rendering_npi as {{ dbt.type_string() }}) as provider_id
  from {{ ref('provider_attribution__stg_input_layer__medical_claim') }} as mc
  left outer join {{ ref('provider_attribution__stg_core__claims_medical_claim') }} as cm
    on mc.claim_id = cm.claim_id
   and mc.claim_line_number = cm.claim_line_number
   and mc.data_source = cm.data_source
  left outer join {{ ref('provider_attribution__stg_reference_data__calendar') }} as cal
    on cast(mc.claim_start_date as date) = cal.full_date
)

, eligible_all_claims as (
  select ac.*
  from all_claim_month as ac
  inner join {{ ref('provider_attribution__stg_core__member_months') }} as mm
    on ac.person_id = mm.person_id
   and ac.claim_year_month = mm.year_month
)

, all_rendering_claims as (
  select
      e.person_id
    , e.provider_id
    , e.encounter_id
    , e.claim_id
    , e.claim_year_month
    , e.claim_year_month_int
    , e.claim_end_date
    , e.allowed_amount
    , coalesce(pc.provider_bucket, 'other_individual') as provider_bucket
    , coalesce(pc.prov_specialty, sp.primary_specialty_description) as prov_specialty
  from eligible_all_claims as e
  inner join {{ ref('provider_attribution__stg_terminology__provider') }} as sp
    on cast(e.provider_id as {{ dbt.type_string() }}) = cast(sp.npi as {{ dbt.type_string() }})
   and lower(trim(sp.entity_type_description)) = 'individual'
  left outer join {{ ref('provider_attribution__provider_classification') }} as pc
    on e.provider_id = pc.provider_id
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
  from claims_12 as c
  where c.provider_bucket in ('pcp', 'npp')
    and c.provider_id is not null
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
  from claims_12 as c
  left outer join step1_benes as s1 on s1.person_id = c.person_id
  where s1.person_id is null
    and c.provider_bucket = 'specialist'
    and c.provider_id is not null
  group by c.person_id, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step2_benes as (
  select distinct person_id from step2
)

, step3 as (
  select
      c.person_id
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 3 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from claims_24 as c
  left outer join step1_benes as s1 on s1.person_id = c.person_id
  left outer join step2_benes as s2 on s2.person_id = c.person_id
  where s1.person_id is null
    and s2.person_id is null
    and c.provider_bucket in ('pcp', 'npp')
    and c.provider_id is not null
  group by c.person_id, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step3_benes as (
  select distinct person_id from step3
)

, step4 as (
  select
      c.person_id
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 4 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from claims_24 as c
  left outer join step1_benes as s1 on s1.person_id = c.person_id
  left outer join step2_benes as s2 on s2.person_id = c.person_id
  left outer join step3_benes as s3 on s3.person_id = c.person_id
  where s1.person_id is null
    and s2.person_id is null
    and s3.person_id is null
    and c.provider_id is not null
  group by c.person_id, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step4_benes as (
  select distinct person_id from step4
)

, assigned_pairs as (
  select person_id, provider_id from step1
  {% if target.type == 'fabric' %}
  union
  {% else %}
  union distinct
  {% endif %}
  select person_id, provider_id from step2
  {% if target.type == 'fabric' %}
  union
  {% else %}
  union distinct
  {% endif %}
  select person_id, provider_id from step3
  {% if target.type == 'fabric' %}
  union
  {% else %}
  union distinct
  {% endif %}
  select person_id, provider_id from step4
)

, step5 as (
  select
      arc.person_id
    , arc.provider_id
    , coalesce(arc.provider_bucket, 'unknown') as provider_bucket
    , arc.prov_specialty
    , 5 as step
    , sum(arc.allowed_amount) as allowed_amount
    , count(distinct arc.encounter_id) as visits
  from all_rendering_claims as arc
  inner join months_24 as m
    on arc.claim_year_month_int = m.year_month_int
  cross join params as p
  left outer join step1_benes as s1 on s1.person_id = arc.person_id
  left outer join step2_benes as s2 on s2.person_id = arc.person_id
  left outer join step3_benes as s3 on s3.person_id = arc.person_id
  left outer join step4_benes as s4 on s4.person_id = arc.person_id
  left outer join assigned_pairs as assigned
    on assigned.person_id = arc.person_id
   and assigned.provider_id = arc.provider_id
  where arc.provider_id is not null
    and arc.claim_end_date <= p.as_of_date
    and s1.person_id is null
    and s2.person_id is null
    and s3.person_id is null
    and s4.person_id is null
    and assigned.provider_id is null
  group by arc.person_id, arc.provider_id, coalesce(arc.provider_bucket, 'unknown'), arc.prov_specialty
)

, all_steps as (
  select * from step1
  union all
  select * from step2
  union all
  select * from step3
  union all
  select * from step4
  union all
  select * from step5
)

select
    person_id
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , case step
      when 1 then '12-month PCP/NPP primary-care HCPCS'
      when 2 then '12-month specialist primary-care HCPCS'
      when 3 then '24-month PCP/NPP primary-care HCPCS'
      when 4 then '24-month primary-care HCPCS (any classification)'
      when 5 then '24-month any rendering NPI'
      else 'Unknown'
    end as step_description
  , allowed_amount
  , visits
from all_steps
