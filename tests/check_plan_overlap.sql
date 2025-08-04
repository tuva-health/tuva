-- This test ensures complete overlap of distinct plan values across pharmacy_claim, medical_claim, and eligibility tables
{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_4', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'warn'
   )
}}

with plan_counts as (
    select 
        plan,
        sum(case when source_table = 'pharmacy_claim' then 1 else 0 end) as in_pharmacy_claim,
        sum(case when source_table = 'medical_claim' then 1 else 0 end) as in_medical_claim,
        sum(case when source_table = 'eligibility' then 1 else 0 end) as in_eligibility
    from (
        select distinct plan, 'pharmacy_claim' as source_table
        from {{ ref('pharmacy_claim') }}
        where plan is not null
        
        union all
        
        select distinct plan, 'medical_claim' as source_table
        from {{ ref('medical_claim') }}
        where plan is not null
        
        union all
        
        select distinct plan, 'eligibility' as source_table
        from {{ ref('eligibility') }}
        where plan is not null
    ) all_plans
    group by plan
),

incomplete_overlap as (
    select 
        plan,
        in_pharmacy_claim,
        in_medical_claim,
        in_eligibility
    from plan_counts
    where not (in_pharmacy_claim > 0 and in_medical_claim > 0 and in_eligibility > 0)
)

select * from incomplete_overlap
