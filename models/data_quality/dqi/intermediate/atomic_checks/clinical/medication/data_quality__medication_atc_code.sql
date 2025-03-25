{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' as table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') as drill_down_value
    , 'ATC_CODE' as field_name
    , case when coalesce(term_1.atc_1_name,term_2.atc_2_name,term_3.atc_3_name,term_4.atc_4_name) is not null then 'valid'
           when m.atc_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.atc_code is not null and coalesce(term_1.atc_1_name,term_2.atc_2_name,term_3.atc_3_name,term_4.atc_4_name) is null
           then 'ATC Code does not join to Terminology rxnorm_to_atc table on any atc level'
           else null end as invalid_reason
    , cast(atc_code as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('medication') }} as m
left outer join {{ ref('terminology__rxnorm_to_atc') }} as term_1 on m.atc_code = term_1.atc_1_name
left outer join {{ ref('terminology__rxnorm_to_atc') }} as term_2 on m.atc_code = term_2.atc_2_name
left outer join {{ ref('terminology__rxnorm_to_atc') }} as term_3 on m.atc_code = term_3.atc_3_name
left outer join {{ ref('terminology__rxnorm_to_atc') }} as term_4 on m.atc_code = term_4.atc_4_name
