{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
   )
}}

select
    cast(lab_result_id as {{ dbt.type_string() }} ) as lab_result_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(accession_number as {{ dbt.type_string() }} ) as accession_number
    , cast(source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(source_code as {{ dbt.type_string() }} ) as source_code
    , cast(source_description as {{ dbt.type_string() }} ) as source_description
    , cast(source_component as {{ dbt.type_string() }} ) as source_component
    , cast(normalized_code_type as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(normalized_code as {{ dbt.type_string() }} ) as normalized_code
    , cast(normalized_description as {{ dbt.type_string() }} ) as normalized_description
    , cast(normalized_component as {{ dbt.type_string() }} ) as normalized_component
    , cast(status as {{ dbt.type_string() }} ) as status
    , cast(result as {{ dbt.type_string() }} ) as result
    , {{ try_to_cast_date('result_date', 'YYYY-MM-DD') }} as result_date
    , {{ try_to_cast_date('collection_date', 'YYYY-MM-DD') }} as collection_date
    , cast(source_units as {{ dbt.type_string() }} ) as source_units
    , cast(normalized_units as {{ dbt.type_string() }} ) as normalized_units
    , cast(source_reference_range_low as {{ dbt.type_string() }} ) as source_reference_range_low
    , cast(source_reference_range_high as {{ dbt.type_string() }} ) as source_reference_range_high
    , cast(normalized_reference_range_low as {{ dbt.type_string() }} ) as normalized_reference_range_low
    , cast(normalized_reference_range_high as {{ dbt.type_string() }} ) as normalized_reference_range_high
    , cast(source_abnormal_flag as {{ dbt.type_int() }} ) as source_abnormal_flag
    , cast(normalized_abnormal_flag as {{ dbt.type_int() }} ) as normalized_abnormal_flag
    , cast(specimen as {{ dbt.type_string() }} ) as specimen
    , cast(ordering_practitioner_id as {{ dbt.type_string() }} ) as ordering_practitioner_id
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('lab_result') }}