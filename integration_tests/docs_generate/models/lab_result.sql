select
      cast(null as {{ dbt.type_string() }} ) as lab_result_id
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as accession_number
    , cast(null as {{ dbt.type_string() }} ) as source_order_type
    , cast(null as {{ dbt.type_string() }} ) as source_order_code
    , cast(null as {{ dbt.type_string() }} ) as source_order_description
    , cast(null as {{ dbt.type_string() }} ) as source_component_type
    , cast(null as {{ dbt.type_string() }} ) as source_component_code
    , cast(null as {{ dbt.type_string() }} ) as source_component_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_order_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_order_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_order_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_component_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_component_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_component_description
    , cast(null as {{ dbt.type_string() }} ) as status
    , cast(null as {{ dbt.type_string() }} ) as result
    , cast(null as {{ dbt.type_timestamp() }}) as result_datetime
    , cast(null as {{ dbt.type_timestamp() }}) as collection_datetime
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
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
limit 0