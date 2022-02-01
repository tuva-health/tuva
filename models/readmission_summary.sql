{{ config(materialized='view', tags='readmissions') }}

select
    a.encounter_id,
    a.patient_id,
    a.admission_sequence,
    a.discharge_date,
    (a.discharge_date - a.admit_date) as length_of_stay,
    a.index_admit_flag,
    b.elective_flag,
    b.discharged_to,
    b.drg,
    b.drg_type,
    b.mdc,
    c.facility_npi,
    c.attending_provider_npi,
    c.paid_amount,
    a.readmit_encounter_id,
    a.readmit_flag,
    a.days_to_readmit,
    a.readmit_0_flag,
    a.readmit_1_flag,
    a.readmit_7_flag,
    a.readmit_15_flag,
    a.readmit_30_flag,
    a.readmit_90_flag,
    a.readmit_180_flag,
    a.readmit_365_flag,
    (d.discharge_date - d.admit_date) as readmit_length_of_stay,
    d.elective_flag as readmit_elective_flag,
    d.discharged_to as readmit_discharged_to,
    d.drg as readmit_drg,
    d.drg_type as readmit_drg_type,
    d.mdc as readmit_mdc,
    e.facility_npi as readmit_facility_npi,
    e.attending_provider_npi as readmit_attending_provider_npi,
    e.paid_amount as readmit_paid_amount   
from {{ ref('hospital_wide_readmission') }} a
left join {{ ref('inpatient_encounter') }} b
    on a.encounter_id = b.encounter_id
left join {{ ref('stg_encounter') }} c
    on a.encounter_id = c.encounter_id
left join {{ ref('inpatient_encounter') }} d
    on a.readmit_encounter_id = d.encounter_id
left join {{ ref('stg_encounter') }} e
    on a.readmit_encounter_id = e.encounter_id