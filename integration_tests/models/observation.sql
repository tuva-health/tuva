select
cast(null as {{ dbt.type_string() }} ) as observation_id
, cast(null as {{ dbt.type_string() }} ) as patient_id
, cast(null as {{ dbt.type_string() }} ) as encounter_id
, cast(null as {{ dbt.type_string() }} ) as panel_id
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as observation_date
, cast(null as {{ dbt.type_string() }} ) as observation_type
, cast(null as {{ dbt.type_string() }} ) as source_code_type
, cast(null as {{ dbt.type_string() }} ) as source_code
, cast(null as {{ dbt.type_string() }} ) as source_description
, cast(null as {{ dbt.type_string() }} ) as normalized_code_type
, cast(null as {{ dbt.type_string() }} ) as normalized_code
, cast(null as {{ dbt.type_string() }} ) as normalized_description
, cast(null as {{ dbt.type_string() }} ) as result
, cast(null as {{ dbt.type_string() }} ) as source_units
, cast(null as {{ dbt.type_string() }} ) as normalized_units
, cast(null as {{ dbt.type_string() }} ) as source_reference_range_low
, cast(null as {{ dbt.type_string() }} ) as source_reference_range_high
, cast(null as {{ dbt.type_string() }} ) as normalized_reference_range_low
, cast(null as {{ dbt.type_string() }} ) as normalized_reference_range_high
, cast(null as {{ dbt.type_string() }} ) as data_source
, cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0