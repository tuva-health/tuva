{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- Here we list all encounters from the stg_encounter model
-- and we augment them with extra fields
-- that are relevant for readmission measures
select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    aa.discharge_disposition_code,
    aa.facility_npi,
    aa.ms_drg_code,
    aa.paid_amount,
    {{ dbt.datediff("aa.admit_date", "aa.discharge_date","day") }} as length_of_stay,
    case
        when bb.encounter_id is not null then 1
	    else 0
    end as index_admission_flag,
    case
        when cc.encounter_id is not null then 1
	    else 0
    end as planned_flag,
    dd.specialty_cohort,
    case
        when aa.discharge_disposition_code = '20' then 1
	    else 0
    end as died_flag,
    ee.diagnosis_ccs,
    ee.disqualified_encounter_flag,
    ee.missing_admit_date_flag,
    ee.missing_discharge_date_flag,
    ee.admit_after_discharge_flag,
    ee.missing_discharge_disposition_code_flag,
    ee.invalid_discharge_disposition_code_flag,
    ee.missing_primary_diagnosis_flag,
    ee.invalid_primary_diagnosis_code_flag,
    ee.no_diagnosis_ccs_flag,
    ee.overlaps_with_another_encounter_flag,
    ee.missing_ms_drg_flag,
    ee.invalid_ms_drg_flag,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from
    {{ ref('readmissions__encounter') }} aa
    left join {{ ref('readmissions__index_admission') }} bb
    on aa.encounter_id = bb.encounter_id
    left join {{ ref('readmissions__planned_encounter') }} cc
    on aa.encounter_id = cc.encounter_id 
    left join {{ ref('readmissions__encounter_specialty_cohort') }} dd
    on aa.encounter_id = dd.encounter_id
    left join {{ ref('readmissions__encounter_data_quality') }} ee
    on aa.encounter_id = ee.encounter_id
