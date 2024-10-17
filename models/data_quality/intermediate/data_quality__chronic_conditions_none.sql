{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with chronic_conditions as 
(
    select distinct patient_id 
    from {{ ref('chronic_conditions__cms_chronic_conditions_long') }}
)

,results as (
    select 
        'Percent of patients without chronic conditions' as data_quality_check
        , sum(case when cccw.patient_id is null then 1 else 0 end) / count(distinct e.patient_id) * 100 as result_count
    from {{ ref('core__patient') }} e
    left join chronic_conditions cccw 
        on e.patient_id = cccw.patient_id
)

select
    results.data_quality_check
    , results.result_count
    , ref_data.analytics_value as medicare_lds_reference
    , 0 as normally_zero 
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from results
left join {{ ref('data_quality__reference_mart_analytics') }} as ref_data 
    on results.data_quality_check = ref_data.analytics_measure