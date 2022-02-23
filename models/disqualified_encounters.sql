
-- Here we list all encounters from the stg_encounter table
-- with data quality flags that may disqualify them from
-- being used for readmission measures 


{{ config(materialized='view') }}


-- Flag some potential data quality issues
-- with all encounters.
-- Note that in this CTE, encounters that have
-- multiple primary diagnoses codes (which shouldn't happen,
-- but it is a potential data quality problem in a dataset)
-- will appear as separate rows, one row per distinct
-- primary diagnosis code associated with the encounter.
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
        when bb.diagnosis_code is null then 1
	else 0
    end as missing_primary_diagnosis,
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
),


-- This lists all encounters with multiple primary
-- diagnosis codes together with the count
-- of distinct diagnosis codes associated with the
-- encounter.
encounters_with_multiple_primary_diagnoses as (
select
    encounter_id,
    count(*) as primary_diagnosis_count
from {{ ref('diagnosis_ccs') }}
where diagnosis_rank = 1
group by encounter_id
having primary_diagnosis_count > 1
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
    invalid_primary_diagnosis_code,
    no_diagnosis_ccs,
    case
        when encounter_id in
	( select encounter_id
	  from encounters_with_multiple_primary_diagnoses )
	    then 1
	else 0
    end as multiple_primary_diagnoses
from encounter_data_quality_issues
)    


-- We group by encounter_id so that encounters with multiple
-- diagnosis codes (and therefore appearing on more than one
-- row in the all_data_quality_flags CTE above) appear
-- on only one row.
-- The output of this has one row per encounter_id in the
-- stg_encounter model.
select
    encounter_id,
    max(diagnosis_ccs) as diagnosis_ccs,
    max(disqualified_encounter) as disqualified_encounter,
    max(missing_admit_date) as missing_admit_date,
    max(missing_discharge_date) as missing_discharge_date,
    max(admit_after_discharge) as admit_after_discharge,
    max(missing_discharge_status_code) as missing_discharge_status_code,
    max(invalid_discharge_status_code) as invalid_discharge_status_code,
    max(missing_primary_diagnosis) as missing_primary_diagnosis,
    max(invalid_primary_diagnosis_code) as invalid_primary_diagnosis_code,
    max(no_diagnosis_ccs) as no_diagnosis_ccs,
    max(multiple_primary_diagnoses) as multiple_primary_diagnoses
from all_data_quality_flags
group by encounter_id
