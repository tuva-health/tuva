{{ config(
    enabled = var('claims_enabled', var('clinical_enabled', False))
) }}

{#
This code ensures all fields that have atomic level data quality checks
run against them are included in the SUMMARY_SK creation list even if they are not mapped to
any downstream marts, and thus not in the crosswalk fields to marts.
#}

with results as (

    select distinct
        table_name as input_layer_table_name
      , claim_type
      , field_name
      , cast(null as {{ dbt.type_string() }}) as mart_name
    from {{ ref('data_quality__data_quality_detail') }}

    union all

    select
        input_layer_table_name
      , claim_type
      , field_name
      , mart_name
    from {{ ref('data_quality__crosswalk_field_to_mart') }}

)

, final as (

    select
        input_layer_table_name
      , claim_type
      , field_name
      , mart_name
      , dense_rank() over (
order by input_layer_table_name, claim_type, field_name) as table_claim_type_field_sk
	, '{{ var('tuva_last_run') }}' as tuva_last_run
    from results
    group by
        input_layer_table_name
      , claim_type
      , field_name
      , mart_name

)

select * from final
