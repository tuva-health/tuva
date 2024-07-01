{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.DISPENSING_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'MEDICATION' AS TABLE_NAME
    ,'Medication ID' as DRILL_DOWN_KEY
    ,IFNULL(MEDICATION_ID, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'RXNORM_CODE' AS FIELD_NAME
    ,case when TERM.RXCUI is not null then 'valid'
          when M.RXNORM_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.RXNORM_CODE is not null and TERM.RXCUI is null
          then 'RX norm code does not join to Terminology rxnorm_to_atc table'
    else null end as INVALID_REASON
    ,CAST(RXNORM_CODE AS VARCHAR(255)) AS FIELD_VALUE
FROM {{ ref('medication')}} M
LEFT JOIN {{ ref('terminology__rxnorm_to_atc')}} TERM on m.RXNORM_CODE = TERM.RXCUI