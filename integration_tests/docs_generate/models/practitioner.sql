select
      cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as npi
    , cast(null as {{ dbt.type_string() }} ) as first_name
    , cast(null as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as practice_affiliation
    , cast(null as {{ dbt.type_string() }} ) as specialty
    , cast(null as {{ dbt.type_string() }} ) as sub_specialty
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0