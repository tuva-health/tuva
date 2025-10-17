{{ config(
     enabled = var('cms_provider_attribution_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_2', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'error'
   )
}}
-- There can only be prospective assignment when retrospective is selected if voluntarily aligned
SELECT *
FROM {{ ref('cms_provider_attribution__int_assignable_beneficiaries') }}
WHERE
    voluntarily_aligned = 0
    AND assignment_methodology = 'prospective'
    AND '{{ var('assignment_methodology') }}' = 'retrospective'