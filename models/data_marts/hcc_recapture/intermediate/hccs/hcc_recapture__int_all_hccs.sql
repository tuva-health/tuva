{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}
with seed_hcc_hierarchy as (
    select
        model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
    from {{ ref('cms_hcc__disease_hierarchy_flat') }}
)

, get_risk_code as (
    select distinct
        person_id
        , payer
        , payment_year
        , model_version
        , risk_model_code
        , row_number() over (partition by person_id, payment_year, model_version order by collection_end_date desc) as month_order
    from {{ ref('cms_hcc__int_demographic_factors') }}
    where lower(factor_type) = 'demographic'
)

, medical_claims as (
-- Use distinct to remove claim line
    select distinct
        person_id
        , payer
        , claim_id
        , rendering_npi
    from {{ ref('core__medical_claim') }}
)

, eligible_hccs as (
    select * from {{ ref('hcc_recapture__int_coded_hccs') }}

    union all

    select * from {{ ref('hcc_recapture__int_suspect_hccs') }}
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
    , case when elig_bene.person_id is not null then 1 else 0 end as eligible_bene_flag
    , eligible_claim_flag
    , med.rendering_npi
    , suspect_hcc_flag
    , case when chronic.chronic_flag = 1 and eligible_claim_flag = 1 then 1 else 0 end as recapturable_flag
    , hcc_type
    , hcc_source
from eligible_hccs as sus
left join seed_hcc_hierarchy as hier
    on sus.hcc_code = hier.hcc_code
    and sus.model_version = hier.model_version
left join chronic_hccs as chronic
    on sus.model_version = chronic.model_version
    and sus.hcc_code = chronic.hcc_code    
left join get_risk_code as rcode
    on sus.person_id = rcode.person_id
    and sus.payer = rcode.payer
    and {{ date_part('year', 'sus.recorded_date') }} = rcode.payment_year - 1
    and sus.model_version = rcode.model_version
    and rcode.month_order = 1
left join medical_claims as med
    on sus.person_id = med.person_id
    and sus.payer = med.payer
    and sus.claim_id = med.claim_id
-- Only include benes eligible for gap closure
left join {{ ref('hcc_recapture__int_eligible_benes') }} as elig_bene
    on sus.person_id = elig_bene.person_id
    and {{ date_part('year', 'sus.recorded_date') }} = elig_bene.collection_year
    and sus.payer = elig_bene.payer
where sus.hcc_code is not null
  -- Replace with cms_hcc__adjustment_rates once that table includes PY 2026 
  and 1 = (case when {{ date_part('year', 'sus.recorded_date') }} >= 2025 and sus.model_version = 'CMS-HCC-V24' then 0 else 1 end)