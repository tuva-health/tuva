{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with disqualified_unpivot as (
    select encounter_id
    , disqualified_reason
    , flagvalue
    from {{ ref('readmissions__encounter_augmented') }} p
    unpivot(
        flagvalue for disqualified_reason in (
            invalid_discharge_disposition_code_flag
            , invalid_ms_drg_flag
            , invalid_primary_diagnosis_code_flag
            , missing_admit_date_flag
            , missing_discharge_date_flag
            , admit_after_discharge_flag
            , missing_discharge_disposition_code_flag
            , missing_ms_drg_flag
            , missing_primary_diagnosis_flag
            , no_diagnosis_ccs_flag
            , overlaps_with_another_encounter_flag
        )
    ) as unpvt
)


select 
 {{ dbt.concat(["'inpatient encounter '", "d.disqualified_reason"]) }} as data_quality_check
, count(distinct encounter_id) as result_count
from disqualified_unpivot d
where flagvalue = 1
group by disqualified_reason

