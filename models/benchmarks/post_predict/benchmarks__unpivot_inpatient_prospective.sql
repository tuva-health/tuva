{{
    config(
        enabled = var('benchmarks_already_created', False) | as_bool
    )
}}

with discharge_locations as (
    select 'snf' as discharge_location union all
    select 'home' as discharge_location union all
    select 'home health' as discharge_location union all
    select 'expired' as discharge_location union all
    select 'transfer/other facility' as discharge_location union all
    select 'ipt rehab' as discharge_location union all
    select 'hospice' as discharge_location union all
    select 'other' as discharge_location
)

, expected_vs_actual as (
    select 'expected' as eva union all
    select 'actual' as eva
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
        and discharge_locations.discharge_location = 'snf' then p.discharge_pred_proba_snf
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'home' then p.discharge_pred_proba_home
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'home health' then p.discharge_pred_proba_home_health
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'expired' then p.discharge_pred_proba_expired
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'transfer/other facility' then p.discharge_pred_proba_transfer_other_facility
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'ipt rehab' then p.discharge_pred_proba_ipt_rehab
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'hospice' then p.discharge_pred_proba_hospice
      when expected_vs_actual.eva = 'expected'
        and discharge_locations.discharge_location = 'other' then p.discharge_pred_proba_other
      else 0
    end as discharge_location_long
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('benchmarks__predict_inpatient_prospective') }} as p
cross join discharge_locations
cross join expected_vs_actual

