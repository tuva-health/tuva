{{
    config(
        enabled = var('benchmarks_already_created', False) | as_bool
    )
}}

WITH inpatient_pred AS (
    SELECT 
            p.encounter_id
          , p.length_of_stay_pred
          , p.readmission_pred
          , p.discharge_location_pred
    FROM {{ var('predictions_inpatient') }} p 
    
)


select e.encounter_id
, i.length_of_stay_pred
, e.length_of_stay as length_of_stay_actual
, e.readmission_numerator as readmission_numerator_actual
, i.readmission_pred 
, e.readmission_denominator
, e.discharge_location as discharge_location_actual
, i.discharge_location_pred
, ce.facility_id
, ce.facility_name
, case when ce.drg_code_type = 'ms-drg' then ce.drg_code else null end as ms_drg_code
, ce.paid_amount
FROM {{ ref('benchmarks__inpatient_input') }}  e
inner join inpatient_pred i on e.encounter_id = i.encounter_id
inner join {{ ref('core__encounter') }} ce on e.encounter_id = ce.encounter_id