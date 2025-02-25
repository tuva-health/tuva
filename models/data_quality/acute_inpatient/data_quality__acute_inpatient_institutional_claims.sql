{{ config(
    enabled = var('claims_enabled', False)
) }}

with all_aip_inst_claims as (

    select
          claim_id
    from {{ ref('data_quality__aip_venn_diagram') }}
    -- Here we define the logic for what constitutes
    -- an acute inpatient institutional claim by pulling
    -- all claim_ids from the aip_venn_diagram table
    -- that meet this criteria:
    where (drg = 1 or bill = 1)

)

, add_all_final_columns as (

    select
          aip.claim_id
        , other_header_values.person_id
        , merge_dates.merge_start_date
        , merge_dates.merge_end_date
        , header_values.assigned_drg_code as drg_code
        , header_values.assigned_diagnosis_code_1 as diagnosis_code_1
        , header_values.assigned_admit_type_code as admit_type_code
        , header_values.assigned_admit_source_code as admit_source_code
        , header_values.assigned_discharge_disposition_code as discharge_disposition_code
        , other_header_values.facility_npi
        , other_header_values.rendering_npi
        , other_header_values.paid_amount
        , other_header_values.usable_person_id
        , merge_dates.usable_merge_dates
        , header_values.usable_drg_code
        , header_values.usable_diagnosis_code_1
        , header_values.usable_admit_type_code
        , header_values.usable_admit_source_code
        , header_values.usable_discharge_disposition_code
        , other_header_values.usable_facility_npi
        , other_header_values.usable_rendering_npi
    from all_aip_inst_claims aip
    left join {{ ref('data_quality__other_header_values') }} other_header_values
        on aip.claim_id = other_header_values.claim_id
    left join {{ ref('data_quality__header_values') }} header_values
        on aip.claim_id = header_values.claim_id
    left join {{ ref('data_quality__claim_grain_calculated_dates') }} merge_dates
        on aip.claim_id = merge_dates.claim_id

)

, add_dq_problem_and_dq_insight_flags as (

    select
          claim_id
        , person_id
        , merge_start_date
        , merge_end_date
        , drg_code
        , diagnosis_code_1
        , admit_type_code
        , admit_source_code
        , discharge_disposition_code
        , facility_npi
        , rendering_npi
        , paid_amount
        , case
              when (usable_person_id = 1 and usable_merge_dates = 1) then 1
              else 0
          end as usable_for_aip_encounter
        , case
              when (
                     (usable_person_id = 0) 
                  or (usable_merge_dates = 0)
                  or (usable_drg_code = 0)
                  or (usable_diagnosis_code_1 = 0)
                  or (usable_admit_type_code = 0)
                  or (usable_admit_source_code = 0)
                  or (usable_discharge_disposition_code = 0)
                  or (usable_rendering_npi = 0)
                  or (usable_facility_npi = 0)
              ) then 1
              else 0
          end as dq_problem
        , usable_person_id
        , usable_merge_dates
        , usable_drg_code
        , usable_diagnosis_code_1
        , usable_admit_type_code
        , usable_admit_source_code
        , usable_discharge_disposition_code
        , usable_facility_npi
        , usable_rendering_npi
    from add_all_final_columns

)

select
      claim_id
    , person_id
    , merge_start_date
    , merge_end_date
    , drg_code
    , diagnosis_code_1
    , admit_type_code
    , admit_source_code
    , discharge_disposition_code
    , facility_npi
    , rendering_npi
    , paid_amount
    , usable_for_aip_encounter
    , dq_problem
    , usable_person_id
    , usable_merge_dates
    , usable_drg_code
    , usable_diagnosis_code_1
    , usable_admit_type_code
    , usable_admit_source_code
    , usable_discharge_disposition_code
    , usable_facility_npi
    , usable_rendering_npi
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_dq_problem_and_dq_insight_flags
