{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
    M.Data_SOURCE
    ,coalesce(M.DISPENSING_DATE,cast('1900-01-01' as date)) AS SOURCE_DATE
    ,'MEDICATION' AS TABLE_NAME
    ,'Medication ID' as DRILL_DOWN_KEY
    , coalesce(medication_id, 'NULL') AS DRILL_DOWN_VALUE
    -- ,M.CLAIM_TYPE AS CLAIM_TYPE
    ,'ATC_CODE' AS FIELD_NAME
    ,case when COALESCE(TERM_1.atc_1_name,TERM_2.atc_2_name,TERM_3.atc_3_name,TERM_4.atc_4_name) is not null then 'valid'
          when M.ATC_CODE is not null then 'invalid'
          else 'null' 
    end as BUCKET_NAME
    ,case when M.ATC_CODE is not null and COALESCE(TERM_1.atc_1_name,TERM_2.atc_2_name,TERM_3.atc_3_name,TERM_4.atc_4_name) is null
          then 'ATC Code does not join to Terminology rxnorm_to_atc table on any atc level'
    else null end as INVALID_REASON
    ,CAST(ATC_CODE AS VARCHAR(255)) AS FIELD_VALUE
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('medication')}} M
LEFT JOIN {{ ref('terminology__rxnorm_to_atc')}} TERM_1 on m.ATC_CODE = TERM_1.atc_1_name
LEFT JOIN {{ ref('terminology__rxnorm_to_atc')}} TERM_2 on m.ATC_CODE = TERM_2.atc_2_name
LEFT JOIN {{ ref('terminology__rxnorm_to_atc')}} TERM_3 on m.ATC_CODE = TERM_3.atc_3_name
LEFT JOIN {{ ref('terminology__rxnorm_to_atc')}} TERM_4 on m.ATC_CODE = TERM_4.atc_4_name