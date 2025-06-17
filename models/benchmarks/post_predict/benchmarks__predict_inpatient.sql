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
          , p.discharge_pred_proba_expired
          , p.discharge_pred_proba_home
          , p.discharge_pred_proba_home_health
          , p.discharge_pred_proba_hospice
          , p.discharge_pred_proba_ipt_rehab
          , p.discharge_pred_proba_other
          , p.discharge_pred_proba_snf
          , p.discharge_pred_proba_transfer_other_facility
    FROM {{ var('predictions_inpatient') }} p 
    
)

,enrollment_flag as (
    select encounter_id
    ,max(enrollment_flag) as max_enrollment_flag
    from {{ ref('core__encounter') }} e
    group by encounter_id 
)


select e.encounter_id
, ce.data_source
, ce.encounter_start_date
, cal.first_day_of_month
, ce.person_id
, i.length_of_stay_pred as expected_los
, e.length_of_stay as actual_los
, e.readmission_numerator as actual_readmission
, i.readmission_pred as expected_readmission
, e.readmission_denominator
, e.discharge_location as actual_discharge_location
, i.discharge_location_pred as expected_discharge_location
, ce.facility_id
, ce.facility_name
, case when ce.drg_code_type = 'ms-drg' then concat(ce.drg_code,' ',ce.drg_description) else null end as ms_drg_code
, ce.paid_amount
, i.discharge_pred_proba_expired
, i.discharge_pred_proba_home
, i.discharge_pred_proba_home_health
, i.discharge_pred_proba_hospice
, i.discharge_pred_proba_ipt_rehab
, i.discharge_pred_proba_other
, i.discharge_pred_proba_snf
, i.discharge_pred_proba_transfer_other_facility
, ef.max_enrollment_flag as enrolled_encounter_flag
FROM {{ ref('benchmarks__inpatient_input') }}  e
inner join inpatient_pred i on e.encounter_id = i.encounter_id
inner join {{ ref('core__encounter') }} ce on e.encounter_id = ce.encounter_id
inner join {{ ref('reference_data__calendar') }} cal on ce.encounter_start_date = cal.full_date
left join enrollment_flag ef on e.encounter_id = ef.encounter_id 