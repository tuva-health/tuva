{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with results as (
    select 
        'Percent of patients without Chronic Conditions' as data_quality_check
        , sum(case when tccw.patient_id is null then 1 else 0 end) / count(distinct e.patient_id) * 100 as result_count
    from core.eligibility e
    left join {{ ref('chronic_conditions__tuva_chronic_conditions_wide')}} tccw 
        on e.patient_id = tccw.patient_id
)

select
    results.data_quality_check
    , results.result_count
    , ref_data.analytics_value as medicare_lds_reference
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from results
left join {{ ref('data_quality__reference_mart_analytics') }} as ref_data 
    on results.data_quality_check = ref_data.analytics_measure