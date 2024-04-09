{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
    | as_bool
   )
}}

with denominator as (

    select 
          patient_id
        , performance_period_begin
        , performance_period_end
        , recorded_date
        , measure_id
        , measure_name
        , measure_version
    from {{ref('quality_measures__int_nqf0053_denominator')}}

)

, value_sets as (
    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
)

, osteo_proccodes as (

    select
          code
        , code_system
        , concept_name
    from value_sets
    where lower(concept_name) in (
          'bone mineral density test'
        , 'bone mineral density tests cpt'
        , 'bone mineral density tests icd10pcs'
        , 'bone mineral density tests hcpcs'
        , 'dexa dual energy xray absorptiometry, bone density'
        , 'central dual energy x-ray absorptiometry (dxa)'
        , 'spinal densitometry x-ray' 
        , 'ultrasonography for densitometry' 
        , 'ct bone density axial'
        , 'peripheral dual-energy x-ray absorptiometry (dxa)'
    )
)


, proc_osteo as (

    select
        patient_id
      , encounter_id
      , procedure_date
      , source_code_type
      , source_code
    from {{ref('quality_measures__stg_core__procedure')}} as procs

    inner join osteo_proccodes
        on procs.source_code = osteo_proccodes.code
        and procs.source_code_type = osteo_proccodes.code_system
)


, proc_osteo_within_range as (

    select proc_osteo.*
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.performance_period_begin
        , denominator.performance_period_end
    from proc_osteo
    inner join denominator
        on proc_osteo.patient_id = denominator.patient_id
        and 
            proc_osteo.procedure_date between
            denominator.recorded_date 
            and
            {{dbt.dateadd (
                      datepart = "month"
                    , interval = +6
                    , from_date_or_timestamp = "denominator.recorded_date"
            )
            }} 
)

, patients_proc_osteo_not_taken as (
    
    select 
          denominator.patient_id
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.recorded_date
        , denominator.performance_period_begin
        , denominator.performance_period_end  
    from denominator
    left join proc_osteo_within_range 
    on denominator.patient_id = proc_osteo_within_range.patient_id
    where proc_osteo_within_range.patient_id is null

)

-- pharmacy_claim begin

, osteo_pharmacy_codes as (

    select
          code
        , code_system
        , concept_name
    from value_sets
    where lower(concept_name) 
        in 
        ( 
          'osteoporosis medications for urology care'
        , 'osteoporosis medication'
        )
)

, pharm_claim_osteo as (

    select
        patient_id
      , dispensing_date
      , ndc_code  
    from {{ref('quality_measures__stg_pharmacy_claim')}} as pharmacy_claims
    inner join osteo_pharmacy_codes
    on pharmacy_claims.ndc_code = osteo_pharmacy_codes.code
        and lower(osteo_pharmacy_codes.code_system) = 'ndc'
)

, pharmacy_claim_osteo_within_range as (

    select pharm_claim_osteo.*
        , patients_proc_osteo_not_taken.measure_id
        , patients_proc_osteo_not_taken.measure_name
        , patients_proc_osteo_not_taken.measure_version
        , patients_proc_osteo_not_taken.performance_period_begin
        , patients_proc_osteo_not_taken.performance_period_end
    from pharm_claim_osteo
    inner join patients_proc_osteo_not_taken
        on pharm_claim_osteo.patient_id = patients_proc_osteo_not_taken.patient_id
        and pharm_claim_osteo.dispensing_date 
            between 
            patients_proc_osteo_not_taken.performance_period_begin and patients_proc_osteo_not_taken.recorded_date

)

-------------------------------------------------------

-- medication begin

, osteo_medication_codes as (

    select
          code
        , code_system
        , concept_name
    from value_sets
    where lower(concept_name)
        in 
        ( 
          'osteoporosis medications for urology care'
        , 'osteoporosis medication'
        )
    /*osteoporosis medication to match the code in CMS doc*/
)

, medication_osteo as (

    select
        patient_id
      , encounter_id
      , prescribing_date
      , source_code
      , source_code_type
      , ndc_code
      , rxnorm_code
  
    from {{ref('quality_measures__stg_core__medication')}} as medications
    inner join osteo_medication_codes
    on medications.source_code = osteo_medication_codes.code
    and medications.source_code_type = osteo_medication_codes.code_system

)

, medication_osteo_within_range as (

    select medication_osteo.*
        , patients_proc_osteo_not_taken.measure_id
        , patients_proc_osteo_not_taken.measure_name
        , patients_proc_osteo_not_taken.measure_version
        , patients_proc_osteo_not_taken.performance_period_begin
        , patients_proc_osteo_not_taken.performance_period_end
    from medication_osteo
    inner join patients_proc_osteo_not_taken
        on medication_osteo.patient_id = patients_proc_osteo_not_taken.patient_id
        and medication_osteo.prescribing_date between 
            patients_proc_osteo_not_taken.recorded_date 
            and 
            {{ dbt.dateadd (
              datepart = "month"
            , interval = +6
            , from_date_or_timestamp = "patients_proc_osteo_not_taken.recorded_date"
            )
            }}

)

------- repeat codes ---------------
-- , bone_fracture_codes_medication as (

--     select
--           code
--         , code_system
--     from value_sets
--     where lower(concept_name) = 'fracture diagnoses'
-- )

-- , bone_fracture_conditions_medication as (
--     select
--           conditions.patient_id
--         , conditions.claim_id
--         , conditions.encounter_id
--         , conditions.recorded_date
--         , conditions.source_code
--         , conditions.source_code_type
--     from conditions
--     inner join bone_fracture_codes_medication
--         on conditions.source_code_type = bone_fracture_codes_medication.code_system
--             and conditions.source_code = bone_fracture_codes_medication.code
--     inner join {{ ref('quality_measures__int_nqf0053__performance_period') }} as pp
--         on conditions.recorded_date 
--             between pp.performance_period_begin and pp.performance_period_end

-- )
------- repeat codes ---------------

, osteo_medication_for_bone_fracture as (

    select 
          medication_osteo_within_range.*
        , bone_fracture_conditions.claim_id
        , bone_fracture_conditions.recorded_date
    from 
        medication_osteo_within_range
    inner join bone_fracture_conditions
        on medication_osteo_within_range.patient_id = bone_fracture_conditions.patient_id
            and medication_osteo_within_range.encounter_id = bone_fracture_conditions.encounter_id


)

, osteo_medication_within_6_months as (

    select 
        *        
    from osteo_medication_for_bone_fracture
    where datediff(month, recorded_date, prescribing_date) <= 6
)

--------------------------------------------------------------------------------------------------------------


, numerator as (
    select
          proc_osteo_within_range.patient_id
        , proc_osteo_within_range.performance_period_begin
        , proc_osteo_within_range.performance_period_end
        , proc_osteo_within_range.measure_id
        , proc_osteo_within_range.measure_name
        , proc_osteo_within_range.measure_version
        , 1 as numerator_flag
    from proc_osteo_within_range

    union all

    select 
          pharmacy_claim_osteo_within_range.patient_id
        , pharmacy_claim_osteo_within_range.performance_period_begin
        , pharmacy_claim_osteo_within_range.performance_period_end
        , pharmacy_claim_osteo_within_range.measure_id
        , pharmacy_claim_osteo_within_range.measure_name
        , pharmacy_claim_osteo_within_range.measure_version
        , 1 as numerator_flag
    from pharmacy_claim_osteo_within_range

    union all

    select 
          medication_osteo_within_range.patient_id
        , medication_osteo_within_range.performance_period_begin
        , medication_osteo_within_range.performance_period_end
        , medication_osteo_within_range.measure_id
        , medication_osteo_within_range.measure_name
        , medication_osteo_within_range.measure_version
        , 1 as numerator_flag
    from medication_osteo_within_range
)

, add_data_types as (

     select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(numerator_flag as integer) as numerator_flag
    from numerator
)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , numerator_flag
from add_data_types