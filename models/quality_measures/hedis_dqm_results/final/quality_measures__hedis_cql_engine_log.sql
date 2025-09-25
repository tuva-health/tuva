{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}

with dedupe as (

    select *
    from {{ ref('quality_measures__int_hedis_cql_engine_log') }}
    where row_num = 1

)

, measure_name as (

    select *
    from {{ ref('quality_measures__measures') }}

)

/* add clean measure name */
, add_data_types as (

    select
          cast(patient as {{ dbt.type_string() }}) as person_id
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name.name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_year as {{ dbt.type_string() }}) as measure_version
        , cast(cql_key as {{ dbt.type_string() }}) as cql_concept_key
        , cast(cql_value as {{ dbt.type_string() }}) as cql_concept_value
        , cast(data_source as {{ dbt.type_string() }}) as data_source
    from dedupe
        left outer join measure_name
            on dedupe.measure_id = measure_name.id
            and dedupe.measure_year = measure_name.version

)


select
      person_id
    , measure_id
    , measure_name
    , measure_version
    , cql_concept_key
    , cql_concept_value
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
