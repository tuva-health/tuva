{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set institutional_missing_column_list = [
    'claim_id'
    , 'claim_line_number'
    , 'patient_id'
    , 'member_id'
    , 'claim_start_date'
    , 'claim_end_date'
    , 'bill_type_code'
    , 'revenue_center_code'
    , 'hcpcs_code'
    , 'rendering_npi'
    , 'billing_npi'
    , 'facility_npi'
    , 'paid_date'
    , 'paid_amount'
    , 'diagnosis_code_type'
    , 'diagnosis_code_1'
    , 'data_source'
] -%}

with institutional_missing as(

 {{ medical_claim_missing_column_check(builtins.ref('medical_claim'), institutional_missing_column_list, 'institutional') }}

)

select
      'medical_claim' as source_table
    , 'institutional' as claim_type
    , 'claim_id' as grain
    ,  claim_id    
    , 'missing_values' as test_category
    , column_checked||' missing' as test_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from institutional_missing
group by
    claim_id
    , column_checked||' missing'