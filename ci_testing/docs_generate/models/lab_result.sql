select
      cast(null as {{ dbt.type_string() }} ) as lab_result_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as accession_number
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as source_description
    , cast(null as {{ dbt.type_string() }} ) as source_component
    , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_component
    , cast(null as {{ dbt.type_string() }} ) as status
    , cast(null as {{ dbt.type_string() }} ) as result
    , cast(null as date) as result_date
    , cast(null as date) as collection_date
    , cast(null as {{ dbt.type_string() }} ) as source_units
    , cast(null as {{ dbt.type_string() }} ) as normalized_units
    , cast(null as {{ dbt.type_string() }} ) as source_reference_range_low
    , cast(null as {{ dbt.type_string() }} ) as source_reference_range_high
    , cast(null as {{ dbt.type_string() }} ) as normalized_reference_range_low
    , cast(null as {{ dbt.type_string() }} ) as normalized_reference_range_high
    , cast(null as {{ dbt.type_int() }} ) as source_abnormal_flag
    , cast(null as {{ dbt.type_int() }} ) as normalized_abnormal_flag
    , cast(null as {{ dbt.type_string() }} ) as specimen
    , cast(null as {{ dbt.type_string() }} ) as ordering_practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0