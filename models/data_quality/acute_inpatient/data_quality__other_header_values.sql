{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id

    , case
        when (
               (max(person_id) is not null) and 
               (max(person_id) = min(person_id))
             ) then max(person_id)
        else null
      end as person_id

    , case
        when (
               (max(person_id) is not null) and 
               (max(person_id) = min(person_id))
             ) then 1
        else 0
      end as usable_person_id

    , case
        when (
               (max(facility_npi) is not null) and 
               (max(facility_npi) = min(facility_npi))
             ) then max(facility_npi)
        else null
      end as facility_npi

    , case
        when (
               (max(facility_npi) is not null) and 
               (max(facility_npi) = min(facility_npi))
             ) then 1
        else 0
      end as usable_facility_npi

    , case
        when (
               (max(rendering_npi) is not null) and 
               (max(rendering_npi) = min(rendering_npi))
             ) then max(rendering_npi)
        else null
      end as rendering_npi

    , case
        when (
               (max(rendering_npi) is not null) and 
               (max(rendering_npi) = min(rendering_npi))
             ) then 1
        else 0
      end as usable_rendering_npi

    , sum(paid_amount) as paid_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }}
group by
      claim_id

