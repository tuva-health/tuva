{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set eligibility_missing_column_list = [
    'patient_id'
    , 'member_id'
    , 'gender'
    , 'race'
    , 'birth_date'
    , 'death_date'
    , 'death_flag'
    , 'enrollment_start_date'
    , 'enrollment_end_date'
    , 'payer'
    , 'payer_type'
    , 'dual_status_code'
    , 'medicare_status_code'
    , 'first_name'
    , 'last_name'
    , 'address'
    , 'city'
    , 'state'
    , 'zip_code'
    , 'phone'
    , 'data_source'
] -%}



with eligibility_missing as(

 {{ eligibility_missing_column_check(builtins.ref('eligibility'), eligibility_missing_column_list) }}

)


select
      'eligibility' as source_table
    , 'all' as claim_type
    , 'patient_id' as grain
    ,  patient_id    
    , 'missing_values' as test_category
    , column_checked||' missing' as test_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from eligibility_missing
group by
    patient_id
    , column_checked||' missing'