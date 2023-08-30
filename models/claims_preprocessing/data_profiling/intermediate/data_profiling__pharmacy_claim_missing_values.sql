{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set pharmacy_claim_missing_column_list = [
    'claim_id'
    , 'claim_line_number'
    , 'patient_id'
    , 'member_id'
    , 'prescribing_provider_npi'
    , 'dispensing_provider_npi'
    , 'dispensing_date'
    , 'ndc_code'
    , 'quantity'
    , 'days_supply'
    , 'refills'
    , 'paid_date'
    , 'paid_amount'
    , 'allowed_amount'
    , 'data_source'
] -%}



with eligibility_missing as(

 {{ pharmacy_claim_missing_column_check(builtins.ref('pharmacy_claim'), pharmacy_claim_missing_column_list) }}

)


select
      'pharmacy_claim' as source_table
    , 'all' as claim_type
    , 'claim_id' as grain
    ,  claim_id    
    , 'missing_values' as test_category
    , column_checked||' missing' as test_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from eligibility_missing
group by
    claim_id
    , column_checked||' missing'