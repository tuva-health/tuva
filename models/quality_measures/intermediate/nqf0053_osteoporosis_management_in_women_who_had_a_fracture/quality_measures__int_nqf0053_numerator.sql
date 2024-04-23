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

, osteo_procedure_codes as (

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
        , 'osteoporosis medication'
    )

)

, procedures_osteo_related as (

    select
        patient_id
      , encounter_id
      , procedure_date
      , source_code_type
      , source_code
    from {{ref('quality_measures__stg_core__procedure')}} as procs
    inner join osteo_procedure_codes
        on procs.source_code = osteo_procedure_codes.code
        and procs.source_code_type = osteo_procedure_codes.code_system

)

, qualifying_procedures as (

    select
          procedures_osteo_related.patient_id
        , procedures_osteo_related.procedure_date
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.recorded_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
    from procedures_osteo_related
    inner join denominator
        on procedures_osteo_related.patient_id = denominator.patient_id
        and 
            procedures_osteo_related.procedure_date between
            denominator.recorded_date 
            and
            {{dbt.dateadd (
                      datepart = "month"
                    , interval = +6
                    , from_date_or_timestamp = "denominator.recorded_date"
                          )
            }} 

)

, denominator_patients_disqualified_from_procedure as (
    
    select 
          denominator.patient_id
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.recorded_date
        , denominator.performance_period_begin
        , denominator.performance_period_end  
    from denominator
    left join qualifying_procedures 
    on denominator.patient_id = qualifying_procedures.patient_id
    where qualifying_procedures.patient_id is null

)

-- pharmacy_claim begin

, osteo_rx_codes as (

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
        , 'bisphosphonates'
        )

)

, pharmacy_claims_osteo_related as (

    select
        patient_id
      , dispensing_date
      , ndc_code  
    from {{ref('quality_measures__stg_pharmacy_claim')}} as pharmacy_claims
    inner join osteo_rx_codes
    on pharmacy_claims.ndc_code = osteo_rx_codes.code
        and lower(osteo_rx_codes.code_system) = 'ndc'

)

, qualifying_pharmacy_claims as (

    select 
          pharmacy_claims_osteo_related.patient_id
        , pharmacy_claims_osteo_related.dispensing_date
        , pharmacy_claims_osteo_related.ndc_code
        , denominator_patients_disqualified_from_procedure.measure_id
        , denominator_patients_disqualified_from_procedure.measure_name
        , denominator_patients_disqualified_from_procedure.measure_version
        , denominator_patients_disqualified_from_procedure.recorded_date
        , denominator_patients_disqualified_from_procedure.performance_period_begin
        , denominator_patients_disqualified_from_procedure.performance_period_end
    from pharmacy_claims_osteo_related
    inner join denominator_patients_disqualified_from_procedure
        on pharmacy_claims_osteo_related.patient_id = denominator_patients_disqualified_from_procedure.patient_id
        and pharmacy_claims_osteo_related.dispensing_date 
            between             
            denominator_patients_disqualified_from_procedure.recorded_date 
                and 
                {{ dbt.dateadd (
                datepart = "month"
                , interval = +6
                , from_date_or_timestamp = "denominator_patients_disqualified_from_procedure.recorded_date"
                )
                }}
)

-- medication begin

, medication_osteo_related as (

    select
        patient_id
      , encounter_id
      , prescribing_date
      , dispensing_date
      , source_code
      , source_code_type
      , ndc_code
      , rxnorm_code
    from {{ref('quality_measures__stg_core__medication')}} as medications
    inner join osteo_rx_codes
        on medications.source_code = osteo_rx_codes.code
        and medications.source_code_type = osteo_rx_codes.code_system

)

, qualifying_medications as (

    select
          medication_osteo_related.patient_id
        , medication_osteo_related.encounter_id
        , denominator_patients_disqualified_from_procedure.measure_id
        , denominator_patients_disqualified_from_procedure.measure_name
        , denominator_patients_disqualified_from_procedure.measure_version
        , denominator_patients_disqualified_from_procedure.recorded_date
        , denominator_patients_disqualified_from_procedure.performance_period_begin
        , denominator_patients_disqualified_from_procedure.performance_period_end
    from medication_osteo_related
    inner join denominator_patients_disqualified_from_procedure
        on medication_osteo_related.patient_id = denominator_patients_disqualified_from_procedure.patient_id
            and coalesce(medication_osteo_related.prescribing_date, medication_osteo_related.dispensing_date) between 
                denominator_patients_disqualified_from_procedure.recorded_date 
                and 
                {{ dbt.dateadd (
                datepart = "month"
                , interval = +6
                , from_date_or_timestamp = "denominator_patients_disqualified_from_procedure.recorded_date"
                )
                }}

)

, numerator as (

    select
          qualifying_procedures.patient_id
        , qualifying_procedures.performance_period_begin
        , qualifying_procedures.performance_period_end
        , qualifying_procedures.measure_id
        , qualifying_procedures.measure_name
        , qualifying_procedures.measure_version
        , recorded_date as evidence_date
        , 1 as numerator_flag
    from qualifying_procedures

    union all

    select 
          qualifying_pharmacy_claims.patient_id
        , qualifying_pharmacy_claims.performance_period_begin
        , qualifying_pharmacy_claims.performance_period_end
        , qualifying_pharmacy_claims.measure_id
        , qualifying_pharmacy_claims.measure_name
        , qualifying_pharmacy_claims.measure_version
        , recorded_date as evidence_date
        , 1 as numerator_flag
    from qualifying_pharmacy_claims

    union all

    select 
          qualifying_medications.patient_id
        , qualifying_medications.performance_period_begin
        , qualifying_medications.performance_period_end
        , qualifying_medications.measure_id
        , qualifying_medications.measure_name
        , qualifying_medications.measure_version
        , recorded_date as evidence_date
        , 1 as numerator_flag
    from qualifying_medications

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
        , cast(null as {{ dbt.type_string() }}) as evidence_value
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