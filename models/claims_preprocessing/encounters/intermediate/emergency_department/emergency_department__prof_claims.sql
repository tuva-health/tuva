{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with first_claim as (
    select *
    from {{ ref('emergency_department__generate_encounter_id') }}
    where encounter_claim_number = 1
)

, join_first_claim_dates as (
    select f.*
    , dat.encounter_end_date
    , dat.encounter_start_date
    from first_claim as f
    inner join {{ ref('emergency_department__start_end_dates') }} as dat on f.encounter_id = dat.encounter_id
)


-- ensuring each claim is only attributed to one encounter with claim_attribution_number
, inst_and_prof as (
select dat.encounter_id
, dat.encounter_start_date
, dat.encounter_end_date
, prof.claim_id
, prof.claim_line_number
from {{ ref('encounters__stg_medical_claim') }} as med
inner join {{ ref('encounters__stg_professional') }} as prof on med.claim_line_id = prof.claim_line_id
inner join join_first_claim_dates as dat on med.patient_data_source_id = dat.patient_data_source_id
and med.start_date between dat.encounter_start_date and dat.encounter_end_date

union all

select dat.encounter_id
, dat.encounter_start_date
, dat.encounter_end_date
, med.claim_id
, med.claim_line_number
from {{ ref('encounters__stg_medical_claim') }} as med
inner join {{ ref('encounters__stg_outpatient_institutional') }} as inst on med.claim_id = inst.claim_id
inner join join_first_claim_dates as dat on med.patient_data_source_id = dat.patient_data_source_id
and med.start_date between dat.encounter_start_date and dat.encounter_end_date
where dat.claim_id <> med.claim_id
)

select distinct encounter_id
, encounter_start_date
, encounter_end_date
, claim_id
, claim_line_number
, row_number() over (partition by claim_id, claim_line_number
order by encounter_id) as claim_attribution_number
from inst_and_prof
