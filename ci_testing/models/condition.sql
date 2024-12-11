select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
 cast(null as {{ dbt.type_string() }} ) as condition_id
, cast(null as {{ dbt.type_string() }} ) as person_id
, cast(null as {{ dbt.type_string() }} ) as patient_id
, cast(null as {{ dbt.type_string() }} ) as encounter_id
, cast(null as {{ dbt.type_string() }} ) as claim_id
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as recorded_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as onset_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as resolved_date
, cast(null as {{ dbt.type_string() }} ) as status
, cast(null as {{ dbt.type_string() }} ) as condition_type
, cast(null as {{ dbt.type_string() }} ) as source_code_type
, cast(null as {{ dbt.type_string() }} ) as source_code
, cast(null as {{ dbt.type_string() }} ) as source_description
, cast(null as {{ dbt.type_string() }} ) as normalized_code_type
, cast(null as {{ dbt.type_string() }} ) as normalized_code
, cast(null as {{ dbt.type_string() }} ) as normalized_description
, cast(null as {{ dbt.type_int() }} ) as condition_rank
, cast(null as {{ dbt.type_string() }} ) as present_on_admit_code
, cast(null as {{ dbt.type_string() }} ) as present_on_admit_description
, cast(null as {{ dbt.type_string() }} ) as data_source
, cast(null as {{ dbt.type_string() }} ) as file_name
, cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
, cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% if target.type == 'fabric' %} {% else %} limit 0 {% endif %}