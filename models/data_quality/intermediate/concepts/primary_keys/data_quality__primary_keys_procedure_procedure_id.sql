{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

WITH valid_conditions AS (
    SELECT
        *
    FROM
        {{ ref('data_quality__procedure_procedure_id') }}
    WHERE
        bucket_name = 'valid'
)
, uniqueness_check as (
        SELECT
                field_value,
                COUNT(*) AS duplicate_count
        FROM
                valid_conditions
        GROUP BY
                field_value
        HAVING
                COUNT(*) > 1
)

, random_sample AS (
    SELECT
        data_source,
        source_date,
        table_name,
        drill_down_key,
        drill_down_value,
        field_name,
        field_value,
        bucket_name,
        row_number() over (order by drill_down_key) as row_number_value
    FROM
        {{ ref('data_quality__procedure_procedure_id') }}
    WHERE
        bucket_name = 'valid'
)

, duplicates_summary AS (
    SELECT
        a.data_source,
        a.source_date,
        a.table_name,
        a.drill_down_key,
        a.drill_down_value,
        a.field_name,
        a.field_value,
        a.bucket_name,
        b.duplicate_count,
        row_number() over (order by drill_down_key) as row_number_value
    FROM
        {{ ref('data_quality__procedure_procedure_id') }} a
    JOIN
        uniqueness_check b on a.field_value = b.field_value
)

SELECT
    *
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM
    duplicates_summary
where row_number_value <= 5

union all

SELECT
    *,
    0 as duplicate_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM
    random_sample
WHERE
    row_number_value <= 5
    and NOT EXISTS (SELECT 1 FROM duplicates_summary)
