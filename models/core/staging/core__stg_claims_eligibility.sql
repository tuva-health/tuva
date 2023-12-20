-- depends_on: {{ ref('data_quality__claims_preprocessing_summary') }}

{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

-- *************************************************
-- This dbt model creates the eligibility table in core.
-- *************************************************




select
         cast(patient_id as {{ dbt.type_string() }} ) as patient_id
       , cast(member_id as {{ dbt.type_string() }} ) as member_id
       , cast(birth_date as date) as birth_date
       , cast(death_date as date) as death_date
       , cast(enrollment_start_date as date ) as enrollment_start_date
       , cast(enrollment_end_date as date ) as enrollment_end_date
       , cast(payer as {{ dbt.type_string() }} ) as payer
       , cast(payer_type as {{ dbt.type_string() }} ) as payer_type
       , cast(plan as {{ dbt.type_string() }} ) as plan
       , cast(original_reason_entitlement_code as {{ dbt.type_string() }} ) as original_reason_entitlement_code
       , cast(dual_status_code as {{ dbt.type_string() }} ) as dual_status_code
       , cast(medicare_status_code as {{ dbt.type_string() }} ) as medicare_status_code
       , cast(data_source as {{ dbt.type_string() }} ) as data_source
       , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__eligibility') }} 
