/* This is taken from the claims mapping guide, which says, 
"The expectation is that the sum of paid_amount, coinsurance_amount, copayment_amount, and deductible_amount will be equivalent to allowed_amount."
*/
{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_2', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
     severity = 'warn'
   )
}}


select 
      (coalesce(rx.paid_amount, 0) +
       coalesce(rx.coinsurance_amount, 0) +
       coalesce(rx.copayment_amount, 0) +
       coalesce(rx.deductible_amount, 0)) -
       coalesce(rx.allowed_amount, 0) as diff
    , rx.paid_amount as paid_amount_check
    , rx.coinsurance_amount as coinsurance_amount_check
    , rx.copayment_amount as copayment_amount_check
    , rx.deductible_amount as deductible_amount_check
    , rx.allowed_amount as allowed_amount_check
    , rx.*

from {{ ref('pharmacy_claim') }} med
where abs(
    (coalesce(rx.paid_amount, 0) +
     coalesce(rx.coinsurance_amount, 0) +
     coalesce(rx.copayment_amount, 0) +
     coalesce(rx.deductible_amount, 0)) -
    coalesce(rx.allowed_amount, 0)
) < 0.01  -- Allow for small rounding differences