{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with first_claim as (
    select *
    from {{ ref('inpatient_hospice__generate_encounter_id') }}
    where encounter_claim_number = 1
)

, join_first_claim_dates as (
    select f.*
    , dat.encounter_end_date
    , dat.encounter_start_date
    from first_claim as f
    inner join {{ ref('inpatient_hospice__start_end_dates') }} as dat on f.encounter_id = dat.encounter_id
)

-- ensuring each prof claim is only attributed to one institutional claim with claim_attribution_number
select dat.encounter_id
, dat.encounter_start_date
, dat.encounter_end_date
, prof.claim_id
, prof.claim_line_number
, row_number() over (partition by prof.claim_line_id
order by dat.encounter_id) as claim_attribution_number
from {{ ref('encounters__stg_medical_claim') }} as med
inner join {{ ref('encounters__stg_professional') }} as prof on med.claim_line_id = prof.claim_line_id
inner join join_first_claim_dates as dat on med.patient_data_source_id = dat.patient_data_source_id
and med.start_date between dat.encounter_start_date and dat.encounter_end_date
