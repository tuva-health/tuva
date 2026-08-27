{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('claims_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'pharmacy_claim_claim_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

with source_rows as (
    select *
    from {{ ref('input_layer__pharmacy_claim') }}
),

aggregated_claims as (
    select
          source_rows.claim_id
        , source_rows.data_source
        , cast(count(*) as {{ dbt.type_bigint() }}) as claim_line_count
        , cast(count(source_rows.person_id) as {{ dbt.type_bigint() }}) as person_id_nonnull_count
        , cast(count(distinct case when source_rows.person_id is not null then source_rows.person_id end) as {{ dbt.type_bigint() }}) as person_id_distinct_count
    from source_rows
    group by
          source_rows.claim_id
        , source_rows.data_source
),

final as (
    select
          aggregated_claims.claim_id
        , aggregated_claims.data_source
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.person_id_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.person_id_nonnull_count > 0"
          ) }} as multiple_person_ids_per_claim
    from aggregated_claims
)

select *
from final
