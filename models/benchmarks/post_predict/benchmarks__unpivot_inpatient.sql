{{
    config(
        enabled = var('benchmarks_already_created', False) | as_bool
    )
}}

with discharge_locations as (
    select 'snf' as discharge_location union all
    select 'home' union all
    select 'home health' union all
    select 'expired' union all
    select 'transfer facility' union all
    select 'ipt rehab' union all
    select 'hospice' union all
    select 'other'
)

, expected_vs_actual as (
    select 'expected' as eva union all
    select 'actual'
)

select
    p.encounter_id
  , p.person_id
  , discharge_locations.discharge_location
  , expected_vs_actual.eva
  , case 
      when expected_vs_actual.eva = 'actual' 
        and discharge_locations.discharge_location = p.actual_discharge_location then 1
      when expected_vs_actual.eva = 'expected' 
        and discharge_locations.discharge_location = p.expected_discharge_location then 1
      else 0
    end as discharge_location_long
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('benchmarks__predict_inpatient') }} as p
cross join discharge_locations
cross join expected_vs_actual
