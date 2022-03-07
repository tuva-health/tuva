
-- Here we list all encounters from the stg_encounter table
-- with data quality flags that may disqualify them from
-- being used for readmission measures 


{{ config(materialized='view') }}



-- Flag several potential data quality issues
-- with all encounters.
-- Every encounter_id from the stg_encounter model
-- will be here. This CTE should have the same
-- number of rows as the stg_encounter model, i.e.,
-- there is a one to one correspondence between the
-- rows in stg_encounter and this CTE.
with encounter_data_quality_issues as (
select
    aa.encounter_id,
    case
        when aa.admit_date is null then 1
	else 0
    end as missing_admit_date_flag,
    case
        when aa.discharge_date is null then 1
	else 0
    end as missing_discharge_date_flag,
    case
        when aa.admit_date > aa.discharge_date then 1
	else 0
    end as admit_after_discharge_flag,
    case
        when aa.discharge_status_code is null then 1
	else 0
    end as missing_discharge_status_code_flag,
    case
        when
	    (aa.discharge_status_code is not null)
	    and
	    (cc.discharge_status_code is null) then 1
	else 0
    end as invalid_discharge_status_code_flag,
    case
        when (  (dd.primary_dx_count is null)
	        or
	        (dd.primary_dx_count = 0)
		or
		(dd.primary_dx_count = 1 and bb.diagnosis_code is null)  )
	     then 1
	else 0
    end as missing_primary_diagnosis_flag,
    case
        when dd.primary_dx_count > 1 then 1
	else 0
    end as multiple_primary_diagnoses_flag,
    case
        when bb.valid_icd_10_cm_flag = 0 then 1
	else 0
    end as invalid_primary_diagnosis_code_flag,
    case
        when (  bb.valid_icd_10_cm_flag = 1
	        and
	        bb.ccs_diagnosis_category is null  )
             then 1
	else 0
    end as no_diagnosis_ccs_flag,
    bb.ccs_diagnosis_category as diagnosis_ccs,
    case
        when aa.encounter_id in (select distinct encounter_id_A
	                         from {{ ref('encounter_overlap') }} )
	     or
	     aa.encounter_id in (select distinct encounter_id_B
	                         from {{ ref('encounter_overlap') }} )
	then 1
	else 0
    end as overlaps_with_another_encounter_flag

from {{ ref('stg_encounter') }} aa
     left join {{ ref('diagnosis_ccs') }} bb
     on aa.encounter_id = bb.encounter_id
     left join {{ ref('discharge_status_code') }} cc
     on aa.discharge_status_code = cc.discharge_status_code
     left join {{ ref('primary_diagnosis_count') }} dd
     on aa.encounter_id = dd.encounter_id
),


-- Here we add a disqualified_encounter_flag.
-- This disqualified_encounter_flag = 1
-- when any of the critical data quality flags
-- from the above CTE are equal to 1.
all_data_quality_flags as (
select
    encounter_id,
    diagnosis_ccs,
    case
        when
	    (missing_admit_date_flag = 1)
	    or
	    (missing_discharge_date_flag = 1)
	    or
	    (admit_after_discharge_flag = 1)
	    or
	    (missing_discharge_status_code_flag = 1)
	    or
	    (invalid_discharge_status_code_flag = 1)
	    or
	    (missing_primary_diagnosis_flag = 1)
	    or
	    (multiple_primary_diagnoses_flag =1)
	    or
	    (invalid_primary_diagnosis_code_flag = 1)
	    or
	    (no_diagnosis_ccs_flag = 1)
	    or
	    (overlaps_with_another_encounter_flag = 1)
	    then 1
	else 0
    end as disqualified_encounter_flag,
    missing_admit_date_flag,
    missing_discharge_date_flag,
    admit_after_discharge_flag,
    missing_discharge_status_code_flag,
    invalid_discharge_status_code_flag,
    missing_primary_diagnosis_flag,
    multiple_primary_diagnoses_flag,
    invalid_primary_diagnosis_code_flag,
    no_diagnosis_ccs_flag,
    overlaps_with_another_encounter_flag
from encounter_data_quality_issues
)    



select *
from all_data_quality_flags
