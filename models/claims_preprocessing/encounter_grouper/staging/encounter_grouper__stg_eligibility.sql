{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
      e.person_id
    , e.birth_date
    , e.gender
    , e.race
    , d.patient_data_source_id
    , row_number() over (partition by d.patient_data_source_id
order by e.enrollment_start_date desc) as patient_row_num
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('claims_normalization__eligibility') }} as e
inner join {{ ref('encounter_grouper__patient_data_source_id') }} as d on e.person_id = d.person_id
and
e.data_source = d.data_source
