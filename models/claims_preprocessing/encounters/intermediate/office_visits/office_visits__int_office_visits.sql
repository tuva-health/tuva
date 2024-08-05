{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

WITH base_encounters AS (
    SELECT DISTINCT
        mc.patient_id,
        mc.rendering_id,
        mc.start_date
    FROM {{ ref('encounters__stg_medical_claim') }} mc
    INNER JOIN {{ ref('encounters__stg_professional') }} p 
        ON mc.claim_id = p.claim_id 
        AND mc.claim_line_number = p.claim_line_number
    WHERE mc.place_of_service_code = 11
),
/*
encounter_ids AS (
    SELECT
        patient_id,
        rendering_id,
        start_date,
        {{ dbt_utils.generate_surrogate_key(['patient_id', 'rendering_id', 'start_date']) }} AS encounter_id
    FROM base_encounters
),
*/
encounter_ids as (
select
    mc.patient_id
    ,mc.rendering_id
    ,mc.start_date
    ,mc.claim_id as encounter_id
    ,row_number() over(partition by mc.patient_id,mc.rendering_id,mc.start_date order by mc.claim_id) as claim_encounter
from {{ ref('encounters__stg_medical_claim') }} mc
inner join base_encounters be on mc.patient_id = be.patient_id
    and mc.rendering_id = be.rendering_id
    and mc.start_date = be.start_date
),

final_encounters AS (
    SELECT
        mc.claim_id,
        mc.claim_line_number,
        ei.encounter_id
    FROM {{ ref('encounters__stg_medical_claim') }} mc
    INNER JOIN {{ ref('encounters__stg_professional') }} p 
        ON mc.claim_id = p.claim_id
        AND mc.claim_line_number = p.claim_line_number
    INNER JOIN encounter_ids ei
        ON mc.patient_id = ei.patient_id
        AND mc.rendering_id = ei.rendering_id
        AND mc.start_date = ei.start_date
        AND ei.claim_encounter = 1
)

SELECT * FROM final_encounters