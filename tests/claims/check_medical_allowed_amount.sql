/* This is taken from the claims mapping guide, which says,
"The expectation is that the sum of paid_amount, coinsurance_amount, copayment_amount, and deductible_amount will be equivalent to allowed_amount."
*/
{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_4', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'warn'
   )
}}

select
      (coalesce(med.paid_amount, 0) +
       coalesce(med.coinsurance_amount, 0) +
       coalesce(med.copayment_amount, 0) +
       coalesce(med.deductible_amount, 0)) -
       coalesce(med.allowed_amount, 0) as diff
    , med.paid_amount as paid_amount_check
    , med.coinsurance_amount as coinsurance_amount_check
    , med.copayment_amount as copayment_amount_check
    , med.deductible_amount as deductible_amount_check
    , med.allowed_amount as allowed_amount_check
    , med.*
from {{ ref('medical_claim') }} as med
where abs(
    (coalesce(med.paid_amount, 0) +
     coalesce(med.coinsurance_amount, 0) +
     coalesce(med.copayment_amount, 0) +
     coalesce(med.deductible_amount, 0)) -
    coalesce(med.allowed_amount, 0)
) > 0.01  -- Allow for small rounding differences
