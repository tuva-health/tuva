{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with conditions as (

    select
          person_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from {{ ref('hcc_suspecting__int_prep_conditions') }}

)

, seed_clinical_concepts as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('hcc_suspecting__clinical_concepts') }}

)

, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , hcc_description
    from {{ ref('hcc_suspecting__hcc_descriptions') }}

)

, billed_hccs as (

    select distinct
          person_id
        , data_source
        , hcc_code
        , current_year_billed
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}

)

/* BEGIN HCC 37 logic */
, ckd_stage_1_or_2 as (

    select
          conditions.person_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
        , row_number() over (
            partition by
                  conditions.person_id
                , conditions.data_source
            order by
                  conditions.recorded_date desc
                , conditions.code desc
          ) as row_num
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) in (
          'chronic kidney disease, stage 1'
        , 'chronic kidney disease, stage 2'
    )

)

, ckd_stage_1_or_2_dedupe as (

    select
          person_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
        , concept_name
    from ckd_stage_1_or_2
    where row_num = 1

)

, diabetes as (

    select
          conditions.person_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
        , row_number() over (
            partition by
                  conditions.person_id
                , conditions.data_source
            order by
                  conditions.recorded_date desc
                , conditions.code desc
          ) as row_num
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) = 'diabetes'
)

, diabetes_dedupe as (

    select
          person_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
        , concept_name
    from diabetes
    where row_num = 1

)

, hcc_37_suspect as (

    select
          diabetes_dedupe.person_id
        , diabetes_dedupe.data_source
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
        , diabetes_dedupe.concept_name as condition_1_concept_name
        , diabetes_dedupe.code as condition_1_code
        , diabetes_dedupe.recorded_date as condition_1_recorded_date
        , ckd_stage_1_or_2_dedupe.concept_name as condition_2_concept_name
        , ckd_stage_1_or_2_dedupe.code as condition_2_code
        , ckd_stage_1_or_2_dedupe.recorded_date as condition_2_recorded_date
    from diabetes_dedupe
        inner join ckd_stage_1_or_2_dedupe
            on diabetes_dedupe.person_id = ckd_stage_1_or_2_dedupe.person_id
            and diabetes_dedupe.data_source = ckd_stage_1_or_2_dedupe.data_source
            /* ensure conditions overlap in the same year */
            and {{ date_part('year', 'diabetes_dedupe.recorded_date') }} = {{ date_part('year', 'ckd_stage_1_or_2_dedupe.recorded_date') }}
        inner join seed_hcc_descriptions
            on hcc_code = '37'

)
/* END HCC 37 logic */

, unioned as (

    select * from hcc_37_suspect

)

, add_billed_flag as (

    select
          unioned.person_id
        , unioned.data_source
        , unioned.hcc_code
        , unioned.hcc_description
        , unioned.condition_1_concept_name
        , unioned.condition_1_code
        , unioned.condition_1_recorded_date
        , unioned.condition_2_concept_name
        , unioned.condition_2_code
        , unioned.condition_2_recorded_date
        , billed_hccs.current_year_billed
    from unioned
        left join billed_hccs
            on unioned.person_id = billed_hccs.person_id
            and unioned.data_source = billed_hccs.data_source
            and unioned.hcc_code = billed_hccs.hcc_code

)

, add_standard_fields as (

    select
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , condition_1_concept_name
        , condition_1_code
        , condition_1_recorded_date
        , condition_2_concept_name
        , condition_2_code
        , condition_2_recorded_date
        , current_year_billed
        , cast('Comorbidity suspect' as {{ dbt.type_string() }}) as reason
        , {{ concat_custom([
            "condition_1_concept_name",
            "' ('",
            "condition_1_code",
            "') on '",
            "condition_1_recorded_date",
            "') and '",
            "condition_2_concept_name",
            "' ('",
            "condition_2_code",
            "') on '",
            "condition_2_recorded_date"
        ]) }} as contributing_factor
        , condition_1_recorded_date as suspect_date
    from add_billed_flag

)


, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(condition_1_concept_name as {{ dbt.type_string() }}) as condition_1_concept_name
        , cast(condition_1_code as {{ dbt.type_string() }}) as condition_1_code
        , cast(condition_1_recorded_date as date) as condition_1_recorded_date
        , cast(condition_2_concept_name as {{ dbt.type_string() }}) as condition_2_concept_name
        , cast(condition_2_code as {{ dbt.type_string() }}) as condition_2_code
        , cast(condition_2_recorded_date as date) as condition_2_recorded_date
        {% if target.type == 'fabric' %}
            , cast(current_year_billed as bit) as current_year_billed
        {% else %}
            , cast(current_year_billed as boolean) as current_year_billed
        {% endif %}
        , cast(reason as {{ dbt.type_string() }}) as reason
        , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
        , cast(suspect_date as date) as suspect_date
    from add_standard_fields

)

select
      person_id
    , data_source
    , hcc_code
    , hcc_description
    , condition_1_concept_name
    , condition_1_code
    , condition_1_recorded_date
    , condition_2_concept_name
    , condition_2_code
    , condition_2_recorded_date
    , current_year_billed
    , reason
    , contributing_factor
    , suspect_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types