{{ config(
     enabled = var('quality_measures_reporting_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
{%- set performance_period_end = var('quality_measure_reporting_period_end') -%}

{%- set performance_period_begin -%}
{{ dbt.dateadd(datepart="month", interval=-27, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}

{%- set measure_id -%}
(select id
from {{ ref('quality_measures_reporting__measures') }}
where id = 'NQF2372')
{%- endset -%}

{%- set measure_name -%}
(select name
from {{ ref('quality_measures_reporting__measures') }}
where id = 'NQF2372')
{%- endset -%}

{%- set measure_version -%}
(select version
from {{ ref('quality_measures_reporting__measures') }}
where id = 'NQF2372')
{%- endset -%}

with patient as (

    select
          patient_id
        , sex
        , birth_date
        , death_date
        , cast({{ performance_period_begin }} as date) as performance_period_begin
        , cast('{{ performance_period_end }}'as date) as performance_period_end
    from {{ ref('quality_measures_reporting__stg_core__patient') }}

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
    from {{ ref('quality_measures_reporting__stg_medical_claim') }}

)

, visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures_reporting__value_sets') }}
    where concept_name in (
          'Office Visit'
        , 'Home Healthcare Services'
        , 'Preventive Care Services Established Office Visit, 18 and Up'
        , 'Preventive Care Services Initial Office Visit, 18 and Up'
        , 'Annual Wellness Visit'
        , 'Telephone Visits'
        , 'Online Assessments'
    )

)

, patient_with_age as (

    select
          patient_id
        , sex
        , birth_date
        , death_date
        , performance_period_begin
        , performance_period_end
        , floor({{ datediff('birth_date', 'performance_period_begin', 'hour') }} / 8766.0) as age
    from patient

)

/*
    Filter patient to living women 51 - 74 years of age
    at the beginning of the measurement period
*/
, patient_filtered as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
        , 1 as denominator_flag
    from patient_with_age
    where lower(sex) = 'female'
        and age between 51 and 74
        and death_date is null

)

/*
    Filter to qualifying visit types
*/
, qualifying_visits as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
    from medical_claim
         inner join visit_codes
         on medical_claim.hcpcs_code = visit_codes.code
    where visit_codes.code_system = 'hcpcs'

)

/*
    Filter to final eligible population/denominator before exclusions
    with a qualifying visit during the measurement period
*/
, eligible_population as (

    select
          patient_filtered.patient_id
        , patient_filtered.age
        , patient_filtered.performance_period_begin
        , patient_filtered.performance_period_end
        , patient_filtered.denominator_flag
    from patient_filtered
         inner join qualifying_visits
         on patient_filtered.patient_id = qualifying_visits.patient_id
    where qualifying_visits.claim_start_date >= patient_filtered.performance_period_begin
        or qualifying_visits.claim_end_date <= patient_filtered.performance_period_end
)

, add_data_types as (

    select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast({{ measure_id }} as {{ dbt.type_string() }}) as measure_id
        , cast({{ measure_name }} as {{ dbt.type_string() }}) as measure_name
        , cast({{ measure_version }} as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from eligible_population

)

 select distinct
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types