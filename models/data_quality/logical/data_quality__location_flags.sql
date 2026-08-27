{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'location_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('input_layer__location') }}
),

provider_rows as (
    select distinct
          npi
    from {{ ref('provider_data__provider') }}
),

final as (
    select
          source_rows.location_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("provider_rows.npi is null", "source_rows.npi is not null") }} as npi_invalid
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
    from source_rows
    left join provider_rows
        on cast(source_rows.npi as {{ string_type }}) = cast(provider_rows.npi as {{ string_type }})
)

select *
from final
