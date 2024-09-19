{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_adhras__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_adhras__performance_period') }}

)
{%- endset -%}

, valid_hospice_palliative as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
  from {{ref('quality_measures__int_shared_exclusions_hospice_palliative')}}
  where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)


, esrd_codes as (

    select
            code
          , code_system
          , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name = 'PQA ESRD'
)

, valid_esrd as (

    select
          ct.patient_id
        , ct.recorded_date as exclusion_date
        , ec.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_core__condition') }} as ct
    inner join esrd_codes as ec
    on coalesce(ct.normalized_code, ct.source_code) = ec.code 
    and 
    coalesce(ct.normalized_code_type, ct.source_code_type) = ec.code_system
     where ct.recorded_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, sacubitril_codes as (

    select
           code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name = 'PQA Sacubitril Valsartan Medications'
)

, sacubitril_pharmacy_claim as (

    select
          pc.patient_id
        , pc.dispensing_date as exclusion_date
        , sc.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pc
    inner join sacubitril_codes as sc
    on pc.ndc_code = sc.code 
    where pc.dispensing_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, sacubitril_medication as (

    select
          cm.patient_id
        , cm.dispensing_date as exclusion_date
        , sc.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_core__medication') }} as cm
    inner join sacubitril_codes as sc
    on cm.source_code = sc.code 
    and 
    cm.source_code_type = sc.code_system
    where cm.dispensing_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, valid_sacubitril as (

    select * from sacubitril_pharmacy_claim
    union all
    select * from sacubitril_medication

)

, exclusions as (

    select * from valid_hospice_palliative
    union all
    select * from valid_esrd
    union all
    select * from valid_sacubitril

)

, add_data_types as (

  select
      distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , 1 as exclusion_flag
  from exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    
from add_data_types
