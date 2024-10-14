{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    m.data_source
    ,coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'MEDICAL_CLAIM' AS table_name
    ,'Claim ID' as drill_down_key
,coalesce(claim_id, 'NULL') AS drill_down_value
    ,m.claim_type as claim_type
    ,'CLAIM_TYPE' AS field_name
    ,case when m.claim_type is null then 'null'
          when term.claim_type is null then 'invalid'
                             else 'valid' end as bucket_name
    ,case
        when m.claim_type is not null and term.claim_type is null then 'Claim Type does not join to Terminology Claim Type table'
        else null
    end as invalid_reason
    ,cast(m.claim_type as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim')}} m
left join {{ ref('terminology__claim_type')}} term on m.claim_type = term.claim_type