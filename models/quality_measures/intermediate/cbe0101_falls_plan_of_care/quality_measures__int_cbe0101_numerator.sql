{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
    | as_bool
   )
}}

with denominator as (

    select 
          patient_id
        , encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_cbe0101_denominator') }}

)

, fallcare_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where code = '0518F' -- Falls plan of care documented

)

, procedures as (

    select
        patient_id
      , procedure_date
      , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, qualifying_procedures as (

    select
          patient_id
        , procedure_date as evidence_date
    from procedures
    inner join fallcare_codes
        on procedures.code = fallcare_codes.code
            and procedures.code_type = fallcare_codes.code_system
            
)

, qualifying_claims as (

    select
          patient_id
        , coalesce(claim_end_date, claim_start_date) as evidence_date
    from {{ ref('quality_measures__stg_medical_claim') }} medical_claim
    inner join fallcare_codes
        on medical_claim.hcpcs_code = fallcare_codes.code
            and lower(fallcare_codes.code_system) = 'hcpcs'

)

, qualifying_cares as (

    select
          patient_id
        , evidence_date
    from qualifying_procedures

    union all

    select
          patient_id
        , evidence_date
    from qualifying_claims

)

, combined_qualifying_patients as (

    select
          qualifying_cares.patient_id
        , qualifying_cares.evidence_date
        , null as evidence_value
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , 1 as numerator_flag
    from qualifying_cares
    inner join denominator
        on qualifying_cares.patient_id = denominator.patient_id
    where evidence_date between
        {{ dbt.dateadd (
                  datepart = "year"
                , interval = -1
                , from_date_or_timestamp = "denominator.encounter_date"
                )
        }}
        and 
        denominator.encounter_date -- within last 12 months of falls screening visit

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
    from combined_qualifying_patients

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
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
