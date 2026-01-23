{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select distinct
      fact.coefficient
    , fact.model_version
    , fact.hcc_code
    , coalesce(hier.hcc_hierarchy_group, 'no hierarchy') as hcc_hierarchy_group
    , coalesce(hier.hcc_hierarchy_group_rank, 1) as hcc_hierarchy_group_rank
    -- TODO: Only reference from the Tuva project in the future to make sure this doesn't get out of sync
    , case
        -- ESRD
        when enrollment_status = 'ESRD' then 'ESRD'
        -- New Enrollee
        when enrollment_status = 'New' then 'E'
        -- Long Term Institutional (INS)
        when institutional_status = 'Yes' then 'INS'
        -- Community NonDual Aged (CNA)
        when medicaid_status = 'No' and orec = 'Aged' then 'CNA'
        -- Community NonDual Disabled (CND)
        when medicaid_status = 'No' and orec = 'Disabled' then 'CND'
        -- Community Full Benefit Dual Aged (CFA)
        when dual_status = 'Full' and orec = 'Aged' then 'CFA'
        -- Community Full Benefit Dual Disabled (CFD)
        when dual_status = 'Full' and orec = 'Disabled' then 'CFD'
        -- Community Partial Benefit Dual Aged (CPA)
        when dual_status = 'Partial' and orec = 'Aged' then 'CPA'
        -- Community Partial Benefit Dual Disabled (CPD)
        when dual_status = 'Partial' and orec = 'Disabled' then 'CPD'
    end as risk_model_code
from {{ ref('cms_hcc__disease_factors') }} as fact
left outer join {{ ref('hcc_recapture__stg_hierarchy') }} as hier
  on fact.model_version = hier.model_version
  and fact.hcc_code = hier.hcc_code
