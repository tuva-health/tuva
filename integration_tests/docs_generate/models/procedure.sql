select
      cast(null as {{ dbt.type_string() }} ) as procedure_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as claim_id
    , cast(null as date) as procedure_date
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as source_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_description
    , cast(null as {{ dbt.type_string() }} ) as modifier_1
    , cast(null as {{ dbt.type_string() }} ) as modifier_2
    , cast(null as {{ dbt.type_string() }} ) as modifier_3
    , cast(null as {{ dbt.type_string() }} ) as modifier_4
    , cast(null as {{ dbt.type_string() }} ) as modifier_5
    , cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0