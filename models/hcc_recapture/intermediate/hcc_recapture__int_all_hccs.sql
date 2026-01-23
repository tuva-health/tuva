{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with seed_hcc_hierarchy as (
    select
          model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
    from {{ ref('hcc_recapture__stg_hierarchy') }}
)

, chronic_hccs as (
    select 1 as hcc_code, 1 as model_version, 1 as chronic_flag, 1 as acute_flag
    -- select 
    --     mpgs.hcc_code
    --     , model_version
    --     , case when acute_condition_flag = 'N' then 1 else 0 end as chronic_flag
    --     , case when acute_condition_flag = 'Y' then 1 else 0 end as acute_flag
    -- from mpgs
)

, get_risk_code as (
select distinct
      person_id
    , payer
    , payment_year
    , model_version
    , risk_model_code
    , row_number() over (partition by person_id, payment_year, model_version order by collection_end_date desc) as month_order
from {{ ref('cms_hcc__int_demographic_factors')}}
where lower(factor_type) = 'demographic'
)

, eligible_claims as (
-- Use distinct to remove claim line
select distinct
    person_id
  , claim_id
  , payer
from {{ref('cms_hcc__int_eligible_conditions')}}
)

, medical_claims as (
-- Use distinct to remove claim line
select distinct 
    person_id
  , payer
  , claim_id
  , rendering_id as rendering_npi
from {{ref('core__medical_claim')}}
)

, include_suspect_hccs as (
select
      person_id
    , payer
    , data_source
    , recorded_date
    , model_version
    , claim_id 
    , hcc_code
    , hcc_description
    , condition_type
    -- Listed as prior coding history since it comes from int_all_conditions, see hcc_suspecting__int_patient_hcc_history for reference
    , 'Prior coding history' as reason
    , 0 as suspect_hcc_flag
-- Not using list_all since it doesn't have claim_id pulled through
-- TODO: Update hcc_suspecting__list_all to have claim ID as well
from {{ ref('hcc_suspecting__int_all_conditions') }}
union all
select
      person_id
    , payer
    , data_source
    , suspect_date as recorded_date
    , model_version
    , null as claim_id
    , hcc_code
    , hcc_description
    , 'suspect' as condition_type
    , reason
    , 1 as suspect_hcc_flag
from {{ ref('hcc_suspecting__list_all') }}
-- Exclude since already included in int_all_conditions
where lower(reason) != 'prior coding history'
)

-- NOTE: Distinct is to remove different recording dates + ICD 10 codes for the same HCC code
select distinct
      sus.person_id
    , sus.payer
    , sus.data_source
    , {{ date_part('year', 'sus.recorded_date') }} as collection_year
    , sus.recorded_date
    , sus.model_version
    , sus.claim_id 
    , sus.hcc_code
    , sus.hcc_description
    , chronic.chronic_flag as hcc_chronic_flag
    , coalesce(hier.hcc_hierarchy_group, 'no hierarchy') as hcc_hierarchy_group
    , coalesce(hier.hcc_hierarchy_group_rank, 1) as hcc_hierarchy_group_rank
    , rcode.risk_model_code
    , sus.condition_type
    , case
        when elig.claim_id is not null then 1
        when sus.suspect_hcc_flag = 1 then 1
        else 0
      end as eligible_claim_indicator
    , case when elig_bene.person_id is not null then 1 else 0 end as eligible_bene
    , med.rendering_npi
    , reason
    , suspect_hcc_flag
from include_suspect_hccs as sus
left join seed_hcc_hierarchy as hier
    on sus.hcc_code = hier.hcc_code
    and sus.model_version = hier.model_version
left join chronic_hccs chronic
    on sus.model_version = chronic.model_version
    and sus.hcc_code = chronic.hcc_code
left join get_risk_code rcode
    on sus.person_id = rcode.person_id
    and sus.payer = rcode.payer
    and {{ date_part('year', 'sus.recorded_date') }} = rcode.payment_year - 1
    and sus.model_version = rcode.model_version
    and rcode.month_order = 1
left join eligible_claims as elig
    on sus.person_id = elig.person_id
    and sus.payer = elig.payer
    and sus.claim_id = elig.claim_id
left join medical_claims as med
    on  sus.person_id = med.person_id
    and sus.payer = med.payer
    and sus.claim_id = med.claim_id
-- Only include benes eligible for gap closure
left join {{ ref('hcc_recapture__stg_eligible_benes')}} elig_bene
    on sus.person_id = elig_bene.person_id
    and {{ date_part('year', 'sus.recorded_date') }}  = elig_bene.collection_year
    and sus.payer = elig_bene.payer
where sus.hcc_code is not null
    -- Replace with cms_hcc__adjustment_rates once that table includes PY 2026 
    and 1 = (case when {{ date_part('year', 'sus.recorded_date') }} >= 2025 and sus.model_version = 'CMS-HCC-V24' then 0 else 1 end)