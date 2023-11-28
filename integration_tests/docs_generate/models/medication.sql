select 
      cast(null as {{ dbt.type_string() }} ) as medication_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as date) as dispensing_date
    , cast(null as date) as prescribing_date
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as source_description
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_string() }} ) as ndc_description
    , cast(null as {{ dbt.type_string() }} ) as rxnorm_code
    , cast(null as {{ dbt.type_string() }} ) as rxnorm_description
    , cast(null as {{ dbt.type_string() }} ) as atc_code
    , cast(null as {{ dbt.type_string() }} ) as atc_description
    , cast(null as {{ dbt.type_string() }} ) as route
    , cast(null as {{ dbt.type_string() }} ) as strength
    , cast(null as {{ dbt.type_int() }} ) as quantity
    , cast(null as {{ dbt.type_string() }} ) as quantity_unit
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0