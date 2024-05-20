select 
      cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_type
    , cast(null as date) as encounter_start_date
    , cast(null as date) as encounter_end_date
    , cast(null as {{ dbt.type_int() }} ) as length_of_stay
    , cast(null as {{ dbt.type_string() }} ) as admit_source_code
    , cast(null as {{ dbt.type_string() }} ) as admit_source_description
    , cast(null as {{ dbt.type_string() }} ) as admit_type_code
    , cast(null as {{ dbt.type_string() }} ) as admit_type_description
    , cast(null as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(null as {{ dbt.type_string() }} ) as discharge_disposition_description
    , cast(null as {{ dbt.type_string() }} ) as attending_provider_id
    , cast(null as {{ dbt.type_string() }} ) as facility_npi
    , cast(null as {{ dbt.type_string() }} ) as primary_diagnosis_code_type
    , cast(null as {{ dbt.type_string() }} ) as primary_diagnosis_code
    , cast(null as {{ dbt.type_string() }} ) as primary_diagnosis_description
    , cast(null as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(null as {{ dbt.type_string() }} ) as ms_drg_description
    , cast(null as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(null as {{ dbt.type_string() }} ) as apr_drg_description
    , cast(null as {{ dbt.type_numeric() }} ) as paid_amount
    , cast(null as {{ dbt.type_numeric() }} ) as allowed_amount
    , cast(null as {{ dbt.type_numeric() }} ) as charge_amount
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0