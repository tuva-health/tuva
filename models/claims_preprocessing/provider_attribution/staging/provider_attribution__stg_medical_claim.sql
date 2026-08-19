{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

with all_encounters as (
    select
        claim_id
      , claim_line_number
      , data_source
      , encounter_id
    from {{ ref('encounters__combined_claim_line_crosswalk') }}
    where claim_line_attribution_number = 1

    union all

    select
        claim_id
      , claim_line_number
      , data_source
      , encounter_id
    from {{ ref('encounters__orphaned_claims') }}
)

select
    cast(med.claim_id as {{ dbt.type_string() }}) as claim_id
  , cast(med.claim_line_number as {{ dbt.type_int() }}) as claim_line_number
  , cast(med.person_id as {{ dbt.type_string() }}) as person_id
  , {{ try_to_cast_date('med.claim_start_date', 'YYYY-MM-DD') }} as claim_start_date
  , {{ try_to_cast_date('med.claim_end_date', 'YYYY-MM-DD') }} as claim_end_date
  , cast(med.allowed_amount as {{ dbt.type_numeric() }}) as allowed_amount
  , cast(med.paid_amount as {{ dbt.type_numeric() }}) as paid_amount
  , cast(med.rendering_npi as {{ dbt.type_string() }}) as rendering_npi
  , cast(med.hcpcs_code as {{ dbt.type_string() }}) as hcpcs_code
  , cast(med.data_source as {{ dbt.type_string() }}) as data_source
  , cast(enc.encounter_id as {{ dbt.type_string() }}) as encounter_id
from {{ ref('normalized__medical_claim') }} as med
inner join {{ ref('service_category__service_category_grouper') }} as srv_group
  on med.claim_id = srv_group.claim_id
  and med.claim_line_number = srv_group.claim_line_number
  and med.data_source = srv_group.data_source
  and srv_group.duplicate_row_number = 1
inner join all_encounters as enc
  on med.claim_id = enc.claim_id
  and med.claim_line_number = enc.claim_line_number
  and med.data_source = enc.data_source
