select
 cast(null as condition_id as {{ dbt.type_string() }} ) as condition_id
, cast(null as patient_id as {{ dbt.type_string() }} ) as patient_id
, cast(null as encounter_id as {{ dbt.type_string() }} ) as encounter_id
, cast(null as claim_id as {{ dbt.type_string() }} ) as claim_id
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as recorded_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as onset_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as resolved_date
, cast(null as status as {{ dbt.type_string() }} ) as status
, cast(null as condition_type as {{ dbt.type_string() }} ) as condition_type
, cast(null as source_code_type as {{ dbt.type_string() }} ) as source_code_type
, cast(null as source_code as {{ dbt.type_string() }} ) as source_code
, cast(null as source_description as {{ dbt.type_string() }} ) as source_description
, cast(null as normalized_code_type as {{ dbt.type_string() }} ) as normalized_code_type
, cast(null as normalized_code as {{ dbt.type_string() }} ) as normalized_code
, cast(null as normalized_description as {{ dbt.type_string() }} ) as normalized_description
, cast(null as condition_rank as {{ dbt.type_int() }} ) as condition_rank
, cast(null as present_on_admit_code as {{ dbt.type_string() }} ) as present_on_admit_code
, cast(null as present_on_admit_description as {{ dbt.type_string() }} ) as present_on_admit_description
, cast(null as data_source as {{ dbt.type_string() }} ) as data_source
, cast(null as tuva_last_run as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0