select
      cast(null as {{ dbt.type_string() }} ) as condition_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as claim_id
    , cast(null as date) as recorded_date
    , cast(null as date) as onset_date
    , cast(null as date) as resolved_date
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
limit 0