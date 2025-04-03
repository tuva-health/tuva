{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with chronic_conditions as 
(
    select distinct person_id 
    from {{ ref('chronic_conditions__cms_chronic_conditions_long') }}
)

,results as (
    select 
        cast('Percent of patients without chronic conditions' as {{dbt.type_string()}}) as data_quality_check
        , sum(case when cccw.person_id is null then 1 else 0 end) / count(distinct e.person_id) * 100 as result_count
    from {{ ref('core__patient') }} e
    left join chronic_conditions cccw 
        on e.person_id = cccw.person_id
)

select
    results.data_quality_check
    , results.result_count
    , ref_data.analytics_value as medicare_lds_reference
    , 0 as normally_zero 
    , cast('{{ var('tuva_last_run') }}' as {{dbt.type_string()}}) as tuva_last_run
    
from results
left join {{ ref('data_quality__reference_mart_analytics') }} as ref_data 
    on results.data_quality_check = ref_data.analytics_measure
