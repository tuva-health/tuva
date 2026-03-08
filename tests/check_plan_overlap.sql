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
        {{ quote_column('plan') }}
        , sum(case when source_table = 'pharmacy_claim' then 1 else 0 end) as in_pharmacy_claim
        , sum(case when source_table = 'medical_claim' then 1 else 0 end) as in_medical_claim
        , sum(case when source_table = 'eligibility' then 1 else 0 end) as in_eligibility
    from (
        select distinct {{ quote_column('plan') }}, 'pharmacy_claim' as source_table
        from {{ ref('pharmacy_claim') }}
        where {{ quote_column('plan') }} is not null

        union all

        select distinct {{ quote_column('plan') }}, 'medical_claim' as source_table
        from {{ ref('medical_claim') }}
        where {{ quote_column('plan') }} is not null

        union all

        select distinct {{ quote_column('plan') }}, 'eligibility' as source_table
        from {{ ref('eligibility') }}
        where {{ quote_column('plan') }} is not null
    ) as all_plans
    group by {{ quote_column('plan') }}
)

, table_presence as (
    select
        max(case when source_table = 'pharmacy_claim' then 1 else 0 end) as has_pharmacy
        , max(case when source_table = 'medical_claim' then 1 else 0 end) as has_medical
    from (
        select distinct 'pharmacy_claim' as source_table
        from {{ ref('pharmacy_claim') }}
        where {{ quote_column('plan') }} is not null

        union all

        select distinct 'medical_claim' as source_table
        from {{ ref('medical_claim') }}
        where {{ quote_column('plan') }} is not null
    ) as table_check
)

, incomplete_overlap as (
    select
        plan_counts.{{ quote_column('plan') }}
        , plan_counts.in_pharmacy_claim
        , plan_counts.in_medical_claim
        , plan_counts.in_eligibility
    from plan_counts
    cross join table_presence
    where plan_counts.in_eligibility > 0
      and not (
          (plan_counts.in_pharmacy_claim > 0 or table_presence.has_pharmacy = 0)
          and (plan_counts.in_medical_claim > 0 or table_presence.has_medical = 0)
      )
)

select * from incomplete_overlap
