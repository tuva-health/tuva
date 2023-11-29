{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
   )
}}

select 
    cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_type as {{ dbt.type_string() }} ) as encounter_type
    , {{ try_to_cast_date('encounter_start_date', 'YYYY-MM-DD') }} as encounter_start_date
    , {{ try_to_cast_date('encounter_end_date', 'YYYY-MM-DD') }} as encounter_end_date
    , cast(length_of_stay as {{ dbt.type_int() }} ) as length_of_stay
    , cast(admit_source_code as {{ dbt.type_string() }} ) as admit_source_code
    , cast(admit_source_description as {{ dbt.type_string() }} ) as admit_source_description
    , cast(admit_type_code as {{ dbt.type_string() }} ) as admit_type_code
    , cast(admit_type_description as {{ dbt.type_string() }} ) as admit_type_description
    , cast(discharge_disposition_code as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(discharge_disposition_description as {{ dbt.type_string() }} ) as discharge_disposition_description
    , cast(attending_provider_id as {{ dbt.type_string() }} ) as attending_provider_id
    , cast(facility_npi as {{ dbt.type_string() }} ) as facility_npi
    , cast(primary_diagnosis_code as {{ dbt.type_string() }} ) as primary_diagnosis_code_type
    , cast(primary_diagnosis_code as {{ dbt.type_string() }} ) as primary_diagnosis_code
    , cast(primary_diagnosis_description as {{ dbt.type_string() }} ) as primary_diagnosis_description
    , cast(ms_drg_code as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(ms_drg_description as {{ dbt.type_string() }} ) as ms_drg_description 
    , cast(apr_drg_code as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(apr_drg_description as {{ dbt.type_string() }} ) as apr_drg_description
    , cast(paid_amount as {{ dbt.type_numeric() }} ) as paid_amount
    , cast(allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
    , cast(charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('encounter') }}