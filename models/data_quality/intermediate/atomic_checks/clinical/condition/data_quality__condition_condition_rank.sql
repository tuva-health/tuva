{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
      m.data_source
    , coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    , 'CONDITION' AS table_name
    , 'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') AS drill_down_value
    , 'CONDITION_RANK' AS field_name
    , case when M.CONDITION_RANK is not null then 'valid' else 'null' end as BUCKET_NAME
    , cast(null as {{ dbt.type_string() }}) as INVALID_REASON
    , CAST(CONDITION_RANK as {{ dbt.type_string() }}) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('condition')}} M
