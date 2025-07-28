/* This is taken from the claims mapping guide, which says, 
"The expectation is that the sum of paid_amount, coinsurance_amount, copayment_amount, and deductible_amount will be equivalent to allowed_amount."
*/
{{ config(severity = 'warn') }}


select 
      (coalesce(paid_amount, 0) + 
       coalesce(coinsurance_amount, 0) + 
       coalesce(copayment_amount, 0) + 
       coalesce(deductible_amount, 0)) - 
       coalesce(allowed_amount, 0) as diff    
    , paid_amount as paid_amount_check
    , coinsurance_amount as coinsurance_amount_check
    , copayment_amount as copayment_amount_check
    , deductible_amount as deductible_amount_check
    , allowed_amount as allowed_amount_check
    , med.*

from {{ ref('medical_claim') }} med
where abs(
    (coalesce(paid_amount, 0) + 
     coalesce(coinsurance_amount, 0) + 
     coalesce(copayment_amount, 0) + 
     coalesce(deductible_amount, 0)) - 
    coalesce(allowed_amount, 0)
) < 0.01  -- Allow for small rounding differences