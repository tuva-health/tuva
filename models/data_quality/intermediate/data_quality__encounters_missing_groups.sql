{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with expected_groups as (
    select 'inpatient' as encounter_group
    union all
    select 'outpatient'
    union all
    select 'office based'
    union all
    select 'other'
)

, actual_groups as (
    select distinct
        encounter_group
    from {{ ref('core__encounter') }}
)

select
    a.encounter_group
    , case
        when a.encounter_group is null then 'missing'
      else 'populated'
    end as missing_encounter_group
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from expected_groups e
left join actual_groups a
    on e.encounter_group = a.encounter_group
