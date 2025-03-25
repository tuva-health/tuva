{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

/* measures should already be at the full eligibility population grain */
with union_measures as (

    {{ dbt_utils.union_relations(

        relations=[
              ref('quality_measures__int_cqm438_long')
            , ref('quality_measures__int_cqm130_long')
            , ref('quality_measures__int_nqf0420_long')
            , ref('quality_measures__int_adh_diabetes_long')
            , ref('quality_measures__int_adhras_long')
            , ref('quality_measures__int_supd_long')
            , ref('quality_measures__int_adh_statins_long')
        ]

    ) }}

)

, patient as (

    select distinct person_id
    from {{ ref('quality_measures__stg_core__patient') }}

)

/* selecting the full patient population as the grain of this table */
, joined as (

    select distinct
          patient.person_id
        , union_measures.denominator_flag
        , union_measures.numerator_flag
        , union_measures.exclusion_flag
        , case
            when union_measures.exclusion_flag = 1 then null
            when union_measures.numerator_flag = 1 then 1
            when union_measures.denominator_flag = 1 then 0
            else null
          end as performance_flag
        , union_measures.evidence_date
        , union_measures.evidence_value
        , union_measures.exclusion_date
        , union_measures.exclusion_reason
        , union_measures.performance_period_begin
        , union_measures.performance_period_end
        , union_measures.measure_id
        , union_measures.measure_name
        , union_measures.measure_version
    from patient
        left outer join union_measures
            on patient.person_id = union_measures.person_id
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(performance_flag as integer) as performance_flag
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
    from joined

)

select
      person_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , performance_flag
    , evidence_date
    , evidence_value
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
