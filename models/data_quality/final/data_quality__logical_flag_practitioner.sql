{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool)
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('stg_input_layer__practitioner') }}
),

provider_rows as (
    select distinct
          npi
        , entity_type_code
    from {{ ref('provider_data__provider') }}
),

final as (
    select
          source_rows.practitioner_id
        , source_rows.npi
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.npi is not null and provider_rows.npi is null") }} as npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.npi is not null and provider_rows.npi is not null and provider_rows.entity_type_code is not null and cast(provider_rows.entity_type_code as " ~ string_type ~ ") != '1'") }} as npi_not_individual
    from source_rows
    left join provider_rows
        on cast(source_rows.npi as {{ string_type }}) = cast(provider_rows.npi as {{ string_type }})
)

select *
from final
