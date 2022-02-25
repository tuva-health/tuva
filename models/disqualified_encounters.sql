
-- Here we list all encounters from the stg_encounter table
-- with data quality flags that may disqualify them from
-- being used for readmission measures 


{{ config(materialized='view') }}



-- Flag some potential data quality issues
-- with all encounters.
with encounter_data_quality_issues as (
select
    aa.encounter_id,
    case
        when aa.admit_date is null then 1
	else 0
    end as missing_admit_date,
    case
        when aa.discharge_date is null then 1
	else 0
    end as missing_discharge_date,
    case
        when aa.admit_date > aa.discharge_date then 1
	else 0
    end as admit_after_discharge,
    case
        when aa.discharge_status_code is null then 1
	else 0
    end as missing_discharge_status_code,
    case
        when cc.code is null then 1
	else 0
    end as invalid_discharge_status_code,
    case
        when dd.primary_dx_count is null then 1
	else 0
    end as missing_primary_diagnosis,
    case
        when dd.primary_dx_count > 1 then 1
	else 0
    end as multiple_primary_diagnoses,
    case
        when bb.valid_icd_10_cm = 0 then 1
	else 0
    end as invalid_primary_diagnosis_code,
    case
        when bb.ccs is null then 1
	else 0
    end as no_diagnosis_ccs,
    bb.ccs as diagnosis_ccs

from {{ var('src_encounter') }} aa
     left join (select *
                from {{ ref('diagnosis_ccs') }}
		where diagnosis_rank = 1 ) bb
     on aa.encounter_id = bb.encounter_id
     left join {{ ref('discharge_status_codes') }} cc
     on aa.discharge_status_code = cc.code
     left join {{ ref('primary_diagnosis_count') }} dd
     on aa.encounter_id = dd.encounter_id
),


-- Here we add new data quality flags to all
-- rows from the encounter_data_quality_issues CTE
-- above.
-- Note that in this CTE, again, encounters that have
-- multiple primary diagnoses codes will appear as
-- separate rows, one row per distinct primary diagnosis
-- code associated with the encounter.
all_data_quality_flags as (
select
    encounter_id,
    diagnosis_ccs,
    case
        when
	    (missing_admit_date = 1)
	    or
	    (missing_discharge_date = 1)
	    or
	    (admit_after_discharge = 1)
	    or
	    (missing_discharge_status_code = 1)
	    or
	    (invalid_discharge_status_code = 1)
	    or
	    (missing_primary_diagnosis = 1)
	    or
	    (multiple_primary_diagnoses =1)
	    or
	    (invalid_primary_diagnosis_code = 1)
	    or
	    (no_diagnosis_ccs = 1)
	    then 1
	else 0
    end as disqualified_encounter,
    missing_admit_date,
    missing_discharge_date,
    admit_after_discharge,
    missing_discharge_status_code,
    invalid_discharge_status_code,
    missing_primary_diagnosis,
    multiple_primary_diagnoses,
    invalid_primary_diagnosis_code,
    no_diagnosis_ccs
from encounter_data_quality_issues
)    



select *
from all_data_quality_flags
