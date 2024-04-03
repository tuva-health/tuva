{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
    | as_bool
   )
}}

with denominator as (

    select * from {{ref('quality_measures__int_nqf0053_denominator')}}

),

/*
medications_osteo as (

    select * 
    from {{ref('quality_measures__stg_core__medication')}}
    where lower(source_description) in 
        (
        'bisphosphonates',
        'alendronate', 
        'alendronate-cholecalciferol', 
        'ibandronate', 
        'risedronate', 
        'zoledronic acid', 
        'teriparatide',
        'denosumab', 
        'abaloparatide', 
        'romosozumab',
        'raloxifine'
        )
),

medications_within_range as(

    select medications_osteo.*
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.performance_period_begin
        , denominator.performance_period_end
    from medications_osteo
    inner join denominator
        on medications_osteo.patient_id = denominator.patient_id
        and medications_osteo.prescribing_date between 
            denominator.performance_period_begin and denominator.performance_period_end

),


pharma_osteo as (

    select *
    from {{ref('quality_measures__stg_pharmacy_claim')}}

),


-- medications_osteo_codes as (
--     select
--           code
--         , code_system
--     from {{ ref('quality_measures__value_sets') }}
--     where code IN ('G8633')
-- ),
*/

osteo_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in ('osteoporosis medication', 'bone mineral density test')
),

proc_osteo as (

    select
        patient_id
      , encounter_id
      , procedure_date
      , source_code_type
      , source_code
    from {{ref('quality_measures__stg_core__procedure')}} as procs

    inner join osteo_codes
        on procs.source_code = osteo_codes.code
        and procs.source_code_type = osteo_codes.code_system

    -- where lower(source_description) in 
    --     (
    --     'central dual energy x-ray absorptiometry (dxa)',
    --     'spinal densitometry x-ray', 
    --     'ultrasonography for densitometry', 
    --     'ct bone density axial', 
    --     'peripheral dual-energy x-ray absorptiometry (dxa)' 
    --     )
    -- or lower(source_description) like '%bone mineral density test%'
),

proc_osteo_within_range as (

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

),

patients_proc_osteo_not_taken as (
    
    select denominator.patient_id
    from denominator
    left join proc_osteo_within_range 
    on denominator.patient_id = proc_osteo_within_range.patient_id
    where proc_osteo_within_range.patient_id is null

),

-------------------------------------------------------------------------
bone_fracture_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) = 'fracture procedures'

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
        -- , bone_fracture_conditions.source_code
        -- bone_fracture_conditions.source_code_type
    from 
        proc_osteo_within_range
    inner join bone_fracture_conditions
        on proc_osteo_within_range.patient_id = bone_fracture_conditions.patient_id
        and proc_osteo_within_range.encounter_id = bone_fracture_conditions.encounter_id

),

osteo_proc_within_6_months as (

    select 
        *        
    from osteo_proc_for_bone_fracture
    where datediff(month, recorded_date, procedure_date) <= 6
)


, numerator as (
    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , procedure_date as evidence_date
        , 'DXA Yes' as evidence_value
        , 1 as numerator_flag
    from osteo_proc_within_6_months
)

, add_data_types as (

     select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
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
    , evidence_date
    , evidence_value
    , numerator_flag
from add_data_types



----------------------------------------------------------------------------------------------
-- select * from osteo_proc_within_6_months


-- proc_osteo_codes as (
--     select
--           code
--         , code_system
--     from {{ ref('quality_measures__value_sets') }}
--     where code IN ('3095F')
-- ),

-- procedure_taken2 as (

--     select 
--         proc_osteo_codes.*,
--         'Yes' as DXA_flag
--     from {{ref('quality_measures__stg_core__procedure')}} procs
--     inner join proc_osteo_codes
--     on procs.source_code = proc_osteo_codes.code
--         and procs.source_code_type = proc_osteo_codes.code_system

-- ),

-- proc_with_dxa_flag as (

--     select a.*, b.DXA_flag
--     from {{ref('quality_measures__stg_core__procedure')}} a
--     left join procedure_taken2 b
--     on  a.procedure_id = b.procedure_id

-- ),

----------------------------------------------------------------------------------------------



