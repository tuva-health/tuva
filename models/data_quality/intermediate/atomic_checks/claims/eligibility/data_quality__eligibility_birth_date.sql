{{ config(
    enabled = var('claims_enabled', False)
) }}

WITH base as (
    SELECT * 
    FROM {{ ref('eligibility')}}

),
unique_field as (
    SELECT DISTINCT member_id
        ,cast(birth_date as {{ dbt.type_string() }}) as field
        ,data_source
    FROM base
),
claim_grain as (
    SELECT member_id
        ,data_source
        ,count(*) as frequency
    from unique_field
    GROUP BY member_id
        ,data_source
),
claim_agg as (
SELECT
    member_id,
    data_source,
    {{ dbt.listagg(measure="coalesce(field, 'null')", delimiter_text="', '", order_by_clause="order by field desc") }} AS field_aggregated
FROM
    unique_field
GROUP BY
    data_source,
    member_id
)
SELECT DISTINCT
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' AS table_name
    ,'Member ID' AS drill_down_key
    ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' AS claim_type
    ,'BIRTH_DATE' AS field_name
    ,CASE 
        WHEN cg.frequency > 1 THEN 'multiple'
        WHEN m.birth_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'invalid'
        WHEN m.birth_date <= cast('1901-01-01' as date) THEN 'invalid'
        WHEN m.birth_date IS NULL THEN 'null'
        ELSE 'valid' 
    END AS bucket_name
    ,CASE 
        WHEN cg.frequency > 1 THEN 'multiple'
        WHEN m.birth_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) THEN 'future'
        WHEN m.birth_date <= cast('1901-01-01' as date) THEN 'too old'
        else null
    END AS invalid_reason
,CAST({{ substring('agg.field_aggregated', 1, 255) }} as {{ dbt.type_string() }}) AS field_value
, '{{ var('tuva_last_run')}}' as tuva_last_run
FROM base m
left join claim_grain cg on m.member_id = cg.member_id and m.data_source = cg.data_source
left join claim_agg agg on m.member_id = agg.member_id and m.data_source = agg.data_source

