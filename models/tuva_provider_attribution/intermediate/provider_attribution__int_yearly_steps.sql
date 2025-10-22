{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

-- Yearly attribution steps (ACO-agnostic) using the CMS logic as a base.
-- Steps:
--   1 = PCP/NPP primary-care HCPCS within the performance year.
--   2 = Specialist primary-care HCPCS within the performance year (only if step 1 failed).
--   3 = PCP/NPP primary-care HCPCS across the expanded 24-month window.
--   4 = Primary-care HCPCS fallback across the expanded window regardless of provider classification.
--   5 = Any rendering NPI across the expanded window (fallback when HCPCS-based assignments fail).

with person_years as (
  select * from {{ ref('provider_attribution__int_person_years') }}
)

, claims as (
  select * from {{ ref('provider_attribution__int_primary_care_claims') }}
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
   and c.provider_id is not null
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
    on s1.person_id = py.person_id
   and s1.performance_year = py.performance_year
  where s1.person_id is null
    and c.provider_id is not null
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step2_benes as (
  select distinct person_id, performance_year from step2
)

, step3 as (
  -- 24-month expanded window: Jan of Y-1 .. Dec of Y, PCP/NPP only; only for benes not in step1/step2
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
    on s1.person_id = py.person_id
   and s1.performance_year = py.performance_year
  left outer join step2_benes as s2
    on s2.person_id = py.person_id
   and s2.performance_year = py.performance_year
  where s1.person_id is null
    and s2.person_id is null
    and c.provider_id is not null
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step3_benes as (
  select distinct person_id, performance_year from step3
)

, step4 as (
  -- 24-month expanded window, primary-care HCPCS without requiring provider classification.
  select
      py.person_id
    , py.performance_year
    , c.provider_id
    , coalesce(c.provider_bucket, 'unknown') as provider_bucket
    , c.prov_specialty
    , 4 as step
    , sum(c.allowed_amount) as allowed_amount
    , count(distinct c.encounter_id) as visits
  from person_years as py
  inner join claims as c
    on py.person_id = c.person_id
   and c.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                  and (py.performance_year * 100 + 12)
  left outer join step1_benes as s1
    on s1.person_id = py.person_id
   and s1.performance_year = py.performance_year
  left outer join step2_benes as s2
    on s2.person_id = py.person_id
   and s2.performance_year = py.performance_year
  left outer join step3_benes as s3
    on s3.person_id = py.person_id
   and s3.performance_year = py.performance_year
  where s1.person_id is null
    and s2.person_id is null
    and s3.person_id is null
    and c.provider_id is not null
  group by py.person_id, py.performance_year, c.provider_id, coalesce(c.provider_bucket, 'unknown'), c.prov_specialty
)

, step4_benes as (
  select distinct person_id, performance_year from step4
)

, step4_pairs as (
  select person_id, performance_year, provider_id from step1
  union
  select person_id, performance_year, provider_id from step2
  union
  select person_id, performance_year, provider_id from step3
  union
  select person_id, performance_year, provider_id from step4
)

, step5 as (
  -- 24-month expanded window, any rendering NPI regardless of HCPCS.
  select
      py.person_id
    , py.performance_year
    , arc.provider_id
    , coalesce(arc.provider_bucket, 'unknown') as provider_bucket
    , arc.prov_specialty
    , 5 as step
    , sum(arc.allowed_amount) as allowed_amount
    , count(distinct arc.encounter_id) as visits
  from person_years as py
  inner join all_rendering_claims as arc
    on py.person_id = arc.person_id
   and arc.claim_year_month_int between ((py.performance_year - 1) * 100 + 1)
                                   and (py.performance_year * 100 + 12)
  left outer join step1_benes as s1
    on s1.person_id = py.person_id
   and s1.performance_year = py.performance_year
  left outer join step2_benes as s2
    on s2.person_id = py.person_id
   and s2.performance_year = py.performance_year
  left outer join step3_benes as s3
    on s3.person_id = py.person_id
   and s3.performance_year = py.performance_year
  left outer join step4_benes as s4
    on s4.person_id = py.person_id
   and s4.performance_year = py.performance_year
  left outer join step4_pairs as p4
    on p4.person_id = py.person_id
   and p4.performance_year = py.performance_year
   and p4.provider_id = arc.provider_id
  where arc.provider_id is not null
    and p4.provider_id is null
  group by py.person_id, py.performance_year, arc.provider_id, coalesce(arc.provider_bucket, 'unknown'), arc.prov_specialty
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
  , performance_year
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
