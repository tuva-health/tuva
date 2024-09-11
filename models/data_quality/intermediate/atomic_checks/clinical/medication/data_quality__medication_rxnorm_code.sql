{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' AS table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') AS drill_down_value
    , 'RXNORM_CODE' as field_name
    , case when term.rxcui is not null then 'valid'
           when m.rxnorm_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.rxnorm_code is not null and term.rxcui is null
           then 'RX norm code does not join to Terminology rxnorm_to_atc table'
           else null end as invalid_reason
    , cast(rxnorm_code as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medication')}} m
left join {{ ref('terminology__rxnorm_to_atc')}} term on m.rxnorm_code = term.rxcui
