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
        and proc_osteo.procedure_date between 
            denominator.performance_period_begin and denominator.performance_period_end

)

, patients_proc_osteo_not_taken as (
    
    select 
          denominator.patient_id
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.performance_period_begin
        , denominator.performance_period_end  
    from denominator
    left join proc_osteo_within_range 
    on denominator.patient_id = proc_osteo_within_range.patient_id
    where proc_osteo_within_range.patient_id is null

)

-------------------------------------------------------------------------
, bone_fracture_codes as (

    select
          code
        , code_system
    from value_sets
    where lower(concept_name) = 'fracture diagnoses'

)

, conditions as (

    select
          patient_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from {{ ref('quality_measures__stg_core__condition')}}

)

, bone_fracture_conditions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.encounter_id
        , conditions.recorded_date
        , conditions.source_code
        , conditions.source_code_type
    from conditions
    inner join bone_fracture_codes
        on conditions.source_code_type = bone_fracture_codes.code_system
            and conditions.source_code = bone_fracture_codes.code

)


, osteo_proc_for_bone_fracture as (

    select 
          proc_osteo_within_range.*
        , bone_fracture_conditions.claim_id
        , bone_fracture_conditions.recorded_date

    from 
        proc_osteo_within_range
    inner join bone_fracture_conditions
        on proc_osteo_within_range.patient_id = bone_fracture_conditions.patient_id
        --  and proc_osteo_within_range.encounter_id = bone_fracture_conditions.encounter_id
        /*The one person in procedure likely to be in numerator has encounter_id empty in procedure as well as condition*/
)

, osteo_proc_within_6_months as (

    select 
        *        
    from osteo_proc_for_bone_fracture
    where datediff(month, recorded_date, procedure_date) <= 6
)

-- pharmacy_claim begin

, osteo_pharmacy_codes as (

    select
          code
        , code_system
        , concept_name
    from value_sets
    where lower(concept_name) in ('osteoporosis medications for urology care')
    /*osteoporosis medication for urology care to match the drugs in CMS doc*/
)

, pharm_claim_osteo as (

    select
        patient_id
      , dispensing_date
      , ndc_code  
    from {{ref('quality_measures__stg_pharmacy_claim')}} as pharmacy_claims
    inner join osteo_pharmacy_codes
    on pharmacy_claims.ndc_code = osteo_pharmacy_codes.code
        -- and pharmacy_claims.code_system = osteo_pharmacy_codes.code_system
        /* this is commented out as there is only NDC code system in pharmacy_claims */
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
        and pharm_claim_osteo.dispensing_date between 
            patients_proc_osteo_not_taken.performance_period_begin and patients_proc_osteo_not_taken.performance_period_end

)

, bone_fracture_codes_pharmacy as (

    select
          code
        , code_system
    from value_sets
    where lower(concept_name) = 'fracture diagnoses'
)

, bone_fracture_conditions_pharmacy as (
    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.encounter_id
        , conditions.recorded_date
        , conditions.source_code
        , conditions.source_code_type
    from conditions
    inner join bone_fracture_codes_pharmacy
        on conditions.source_code_type = bone_fracture_codes_pharmacy.code_system
            and conditions.source_code = bone_fracture_codes_pharmacy.code

)

--------------------------

, osteo_pharmacy_for_bone_fracture as (

    select 
          pharmacy_claim_osteo_within_range.*
        , bone_fracture_conditions_pharmacy.claim_id
        , bone_fracture_conditions_pharmacy.recorded_date
    from 
        pharmacy_claim_osteo_within_range
    inner join bone_fracture_conditions_pharmacy
        on pharmacy_claim_osteo_within_range.patient_id = bone_fracture_conditions_pharmacy.patient_id

)

, osteo_pharmacy_within_6_months as (

    select 
        *        
    from osteo_pharmacy_for_bone_fracture
    where datediff(month, dispensing_date, recorded_date) <= 6
)


, numerator as (
    select
          osteo_proc_within_6_months.patient_id
        , osteo_proc_within_6_months.performance_period_begin
        , osteo_proc_within_6_months.performance_period_end
        , osteo_proc_within_6_months.measure_id
        , osteo_proc_within_6_months.measure_name
        , osteo_proc_within_6_months.measure_version
        -- , osteo_proc_within_6_months.procedure_date as evidence_date
        -- , 'DXA Yes' as evidence_value
        , 1 as numerator_flag
    from osteo_proc_within_6_months

    union all

    select 
          osteo_pharmacy_within_6_months.patient_id
        , osteo_pharmacy_within_6_months.performance_period_begin
        , osteo_pharmacy_within_6_months.performance_period_end
        , osteo_pharmacy_within_6_months.measure_id
        , osteo_pharmacy_within_6_months.measure_name
        , osteo_pharmacy_within_6_months.measure_version
        -- , osteo_pharmacy_within_6_months.dispensing_date as evidence_date
        -- , 'Pharmacy_claims' as evidence_value
        , 1 as numerator_flag
    from osteo_pharmacy_within_6_months
)

, add_data_types as (

     select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        -- , cast(evidence_date as date) as evidence_date
        -- , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
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
    -- , evidence_date
    -- , evidence_value
    , numerator_flag
from add_data_types

