{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with first_claim as (

    select
          encounter_id
        , patient_data_source_id
    from {{ ref('inpatient_substance_use__generate_encounter_id') }}
    where encounter_claim_number = 1

)

, join_first_claim_dates as (

    select
          f.encounter_id
        , f.patient_data_source_id
        , dat.encounter_end_date
        , dat.encounter_start_date
    from first_claim as f
    inner join {{ ref('inpatient_substance_use__start_end_dates') }} as dat
        on f.encounter_id = dat.encounter_id

)

-- ensuring each prof claim is only attributed to one institutional claim with claim_attribution_number
select
      dat.encounter_id
    , dat.encounter_start_date
    , dat.encounter_end_date
    , med.claim_id
    , med.claim_line_number
    , row_number() over (
        partition by med.claim_id, med.claim_line_number, med.data_source
        order by dat.encounter_id
      ) as claim_attribution_number
from {{ ref('encounters__stg_medical_claim') }} as med
inner join {{ ref('encounters__prof_and_lower_priority') }} as plp
    on med.claim_id = plp.claim_id
    and med.claim_line_number = plp.claim_line_number
    and med.data_source = plp.data_source
inner join join_first_claim_dates as dat
    on med.patient_data_source_id = dat.patient_data_source_id
    and med.start_date between dat.encounter_start_date and dat.encounter_end_date