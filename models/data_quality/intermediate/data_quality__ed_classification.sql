{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
  select 
    max(case when ed_classification_description = 'Emergent, ED Care Needed, Preventable/Avoidable' then 1 else 0 end) as ed_care_needed_preventable,
    max(case when ed_classification_description = 'Emergent, Primary Care Treatable' then 1 else 0 end) as pc_treatable,
    max(case when ed_classification_description = 'Emergent, ED Care Needed, Not Preventable/Avoidable' then 1 else 0 end) as ed_care_needed_not_preventable,
    max(case when ed_classification_description = 'Non-Emergent' then 1 else 0 end) as non_emergent
  from {{ ref('ed_classification__summary') }}
)

,final as (
select 'ed classification missing ed_care_needed_preventable' as data_quality_check,
       case when ed_care_needed_preventable = 0 then 1 else 0 end as result_count
from cte

union all

select 'ed classification missing pc_treatable' as data_quality_check,
       case when pc_treatable = 0 then 1 else 0 end as result_count
from cte

union all

select 'ed classification missing ed_care_needed_not_preventable' as data_quality_check,
       case when ed_care_needed_not_preventable = 0 then 1 else 0 end as result_count
from cte

union all

select 'ed classification missing non_emergent' as data_quality_check,
       case when non_emergent = 0 then 1 else 0 end as result_count
from cte
)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final