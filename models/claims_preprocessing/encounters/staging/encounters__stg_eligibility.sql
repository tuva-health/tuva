{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
      e.patient_id
    , e.birth_date
    , e.gender
    , e.race
    , d.patient_data_source_id
    , row_number() over (partition by d.patient_data_source_id order by e.enrollment_start_date desc) patient_row_num
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__eligibility') }} e
inner join {{ ref('encounters__patient_data_source_id') }} d on e.patient_id = d.patient_id
and
e.data_source = d.data_source