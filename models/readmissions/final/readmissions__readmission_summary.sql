{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

-- Here calculate days to readmission for encounters
-- that had a readmission and create readmission flags


-- We create the encounter_sequence integer count
-- which keeps track of what number of encounter each
-- encounter is for a given patient
with encounter_sequence as (
select
    *
    , row_number() over (
        partition by person_id
order by admit_date, discharge_date
    ) as encounter_seq
from {{ ref('readmissions__encounter_augmented') }}
where disqualified_encounter_flag = 0
)


, readmission_calc as (
select
    aa.encounter_id
    , aa.person_id
    , aa.admit_date
    , aa.discharge_date
    , aa.discharge_disposition_code
    , aa.facility_id
    , aa.drg_code_type
    , aa.drg_code
    , aa.paid_amount
    , aa.length_of_stay
    , aa.index_admission_flag
    , aa.planned_flag
    , aa.specialty_cohort
    , aa.died_flag
    , aa.diagnosis_ccs
    , case
        when bb.encounter_id is not null then 1
	    else 0
    end as had_readmission_flag
    , {{ dbt.datediff("aa.discharge_date", "bb.admit_date","day") }} as days_to_readmit
    , case
        when ({{ dbt.datediff("aa.discharge_date", "bb.admit_date","day") }}) <= 30 then 1
	    else 0
    end as readmit_30_flag
    , case
        when
	    (({{ dbt.datediff("aa.discharge_date", "bb.admit_date", "day") }}) <= 30) and (bb.planned_flag = 0) then 1
	    else 0
    end as unplanned_readmit_30_flag
    , bb.encounter_id as readmission_encounter_id
    , bb.admit_date as readmission_admit_date
    , bb.discharge_date as readmission_discharge_date
    , bb.discharge_disposition_code as readmission_discharge_disposition_code
    , bb.facility_id as readmission_facility
    , bb.drg_code_type as readmission_drg_code_type
    , bb.drg_code as readmission_drg
    , bb.length_of_stay as readmission_length_of_stay
    , bb.index_admission_flag as readmission_index_admission_flag
    , bb.planned_flag as readmission_planned_flag
    , bb.specialty_cohort as readmission_specialty_cohort
    , bb.died_flag as readmission_died_flag
    , bb.diagnosis_ccs as readmission_diagnosis_ccs
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from
    encounter_sequence as aa
    left outer join encounter_sequence as bb
    on aa.person_id = bb.person_id
    and aa.encounter_seq + 1 = bb.encounter_seq
)

select *
from readmission_calc
