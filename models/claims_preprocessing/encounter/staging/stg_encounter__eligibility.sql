{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select
      e.person_id
    , e.birth_date
    , e.sex
    , e.race
    , d.patient_data_source_id
    , row_number() over (partition by d.patient_data_source_id
order by e.enrollment_start_date desc) as patient_row_num
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('normalized__eligibility') }} as e
inner join {{ ref('normalized__patient_data_source_id') }} as d on e.person_id = d.person_id
and
e.data_source = d.data_source
