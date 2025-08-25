{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT 
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' AS table_name
    ,'Member ID | Enrollment Start Date' AS drill_down_key
        ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' AS claim_type
    ,'CITY' AS field_name
    ,case when m.city is  null then 'null'
                             else 'valid' end as bucket_name
    ,cast(null as {{ dbt.type_string() }}) as invalid_reason
    ,CAST(city as {{ dbt.type_string() }}) AS field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('eligibility')}} m