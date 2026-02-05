{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT 
    p.person_id
  , {{ concat_strings(["p.person_id", "'|'", "p.data_source"]) }} as person_sk
  , p.name_suffix
  , p.first_name
  , p.middle_name
  , p.last_name
  , p.sex
  , p.race
  , p.birth_date
  , p.death_date
  , p.death_flag
  , p.address
  , p.city
  , p.state
  , p.zip_code
  , p.county
  , p.latitude
  , p.longitude
  , p.phone
  , p.email
  , p.ethnicity
  , p.data_source
  , p.age
  , p.age_group
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('semantic_layer__stg_core__patient')}} as p
