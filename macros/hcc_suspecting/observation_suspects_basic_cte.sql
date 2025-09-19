{% macro hcc_suspecting__observation_suspects_basic_cte() %}

with conditions as (
    select person_id, recorded_date, condition_type, code_type, code, data_source
    from {{ ref('hcc_suspecting__int_prep_conditions') }}
),
observations as (
    select person_id, observation_date, result, code_type, code, data_source
    from {{ ref('hcc_suspecting__stg_core__observation') }}
),
numeric_observations as (
    select
          person_id
        , observation_date
        {% if target.type in ['fabric', 'duckdb', 'databricks'] %}
        , TRY_CAST(result AS {{ dbt.type_numeric() }}) AS result
        {% else %}
        , CAST(result as {{ dbt.type_numeric() }}) as result
        {% endif %}
        , code_type
        , code
        , data_source
    from observations
    {% if target.type == 'fabric' %}
        WHERE result LIKE '%.%' OR result LIKE '%[0-9]%'
        AND result NOT LIKE '%[^0-9.]%'
    {% else %}
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}
    {% endif %}
),
seed_clinical_concepts as (
    select concept_name, code, code_system
    from {{ ref('hcc_suspecting__clinical_concepts') }}
),
seed_hcc_descriptions as (
    select distinct hcc_code, hcc_description
    from {{ ref('hcc_suspecting__hcc_descriptions') }}
),
billed_hccs as (
    select distinct person_id, data_source, hcc_code, current_year_billed
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}
),
depression_assessment as (
    select n.person_id, n.observation_date, n.result, n.code_type, n.code, n.data_source, s.concept_name
    from numeric_observations n
    join seed_clinical_concepts s
      on n.code_type = s.code_system and n.code = s.code
    where lower(s.concept_name) = 'depression assessment (phq-9)'
),
diabetes as (
    select c.person_id, c.recorded_date, c.condition_type, c.code_type, c.code, c.data_source, s.concept_name
    from conditions c
    join seed_clinical_concepts s
      on c.code_type = s.code_system and c.code = s.code
    where lower(s.concept_name) = 'diabetes'
),
hypertension as (
    select c.person_id, c.recorded_date, c.condition_type, c.code_type, c.code, c.data_source, s.concept_name
    from conditions c
    join seed_clinical_concepts s
      on c.code_type = s.code_system and c.code = s.code
    where lower(s.concept_name) = 'essential hypertension'
),
obstructive_sleep_apnea as (
    select c.person_id, c.recorded_date, c.condition_type, c.code_type, c.code, c.data_source, s.concept_name
    from conditions c
    join seed_clinical_concepts s
      on c.code_type = s.code_system and c.code = s.code
    where lower(s.concept_name) = 'obstructive sleep apnea'
),
bmi_over_30_with_osa as (
    select
          n.person_id
        , n.data_source
        , n.observation_date
        , n.result as observation_result
        , o.code as condition_code
        , o.recorded_date as condition_date
        , o.concept_name as condition_concept_name
        , h.hcc_code
        , h.hcc_description
    from numeric_observations n
    join seed_clinical_concepts s
      on n.code_type = s.code_system and n.code = s.code
    join obstructive_sleep_apnea o
      on n.person_id = o.person_id
     and {{ date_part('year', 'n.observation_date') }} = {{ date_part('year', 'o.recorded_date') }}
    join seed_hcc_descriptions h on hcc_code = '48'
    where lower(s.concept_name) = 'bmi' and n.result >= 30
),
bmi_over_35_with_diabetes as (
    select
          n.person_id
        , n.data_source
        , n.observation_date
        , n.result as observation_result
        , d.code as condition_code
        , d.recorded_date as condition_date
        , d.concept_name as condition_concept_name
        , h.hcc_code
        , h.hcc_description
    from numeric_observations n
    join seed_clinical_concepts s
      on n.code_type = s.code_system and n.code = s.code
    join diabetes d
      on n.person_id = d.person_id
     and {{ date_part('year', 'n.observation_date') }} = {{ date_part('year', 'd.recorded_date') }}
    join seed_hcc_descriptions h on hcc_code = '48'
    where lower(s.concept_name) = 'bmi' and n.result >= 35
),
bmi_over_35_with_hypertension as (
    select
          n.person_id
        , n.data_source
        , n.observation_date
        , n.result as observation_result
        , htn.code as condition_code
        , htn.recorded_date as condition_date
        , htn.concept_name as condition_concept_name
        , h.hcc_code
        , h.hcc_description
    from numeric_observations n
    join seed_clinical_concepts s
      on n.code_type = s.code_system and n.code = s.code
    join hypertension htn
      on n.person_id = htn.person_id
     and {{ date_part('year', 'n.observation_date') }} = {{ date_part('year', 'htn.recorded_date') }}
    join seed_hcc_descriptions h on hcc_code = '48'
    where lower(s.concept_name) = 'bmi' and n.result >= 35
),
bmi_over_40 as (
    select
          n.person_id
        , n.data_source
        , n.observation_date
        , n.result as observation_result
        , cast(null as {{ dbt.type_string() }}) as condition_code
        , cast(null as date) as condition_date
        , cast(null as {{ dbt.type_string() }}) as condition_concept_name
        , h.hcc_code
        , h.hcc_description
    from numeric_observations n
    join seed_clinical_concepts s
      on n.code_type = s.code_system and n.code = s.code
    join seed_hcc_descriptions h on hcc_code = '48'
    where lower(s.concept_name) = 'bmi' and n.result >= 40
),
hcc_48_unioned as (
    select * from bmi_over_30_with_osa
    union all
    select * from bmi_over_35_with_diabetes
    union all
    select * from bmi_over_35_with_hypertension
    union all
    select * from bmi_over_40
),
hcc_48_suspect as (
    select
          person_id
        , data_source
        , observation_date
        , observation_result
        , condition_code
        , condition_date
        , condition_concept_name
        , hcc_code
        , hcc_description
        , {{ concat_custom([
              "'BMI result '", "observation_result",
              "case",
              " when condition_code is null then '' ",
              " else " ~ concat_custom([
                    "' with '","condition_concept_name","'('","condition_code","' on '","condition_date","')'"
                ]) ~
              " end"
          ]) }} as contributing_factor
    from hcc_48_unioned
),
eligible_depression_assessments as (
    select
          d.person_id
        , d.observation_date
        , d.result
        , d.code_type
        , d.code
        , d.data_source
        , d.concept_name
        , row_number() over (
              partition by d.person_id, d.data_source
              order by case when d.observation_date is null then 1 else 0 end, d.observation_date desc
          ) as assessment_order
    from depression_assessment d
),
depression_assessments_ordered as (
    select
          person_id
        , observation_date
        , code_type
        , code
        , data_source
        , concept_name
        , result
        , row_number() over (
              partition by person_id, data_source
              order by case when result is null then 1 else 0 end, result desc
          ) as result_order
    from eligible_depression_assessments
    where assessment_order <= 3
),
hcc_155_suspect as (
    select
          dao.person_id
        , dao.data_source
        , dao.observation_date
        , dao.result as observation_result
        , cast(null as {{ dbt.type_string() }}) as condition_code
        , cast(null as date) as condition_date
        , dao.concept_name as condition_concept_name
        , h.hcc_code
        , h.hcc_description
        , {{ concat_custom(["'PHQ-9 result '","dao.result","' on '","dao.observation_date"]) }} as contributing_factor
    from depression_assessments_ordered dao
    join seed_hcc_descriptions h on hcc_code = '155'
    where result_order = 1 and dao.result >= 15
),
unioned as (
    select * from hcc_48_suspect
    union all
    select * from hcc_155_suspect
),
add_billed_flag as (
    select
          u.person_id
        , u.data_source
        , u.observation_date
        , u.observation_result
        , u.condition_code
        , u.condition_date
        , u.condition_concept_name
        , u.hcc_code
        , u.hcc_description
        , u.contributing_factor
        , b.current_year_billed
    from unioned u
    left join billed_hccs b
      on u.person_id = b.person_id
     and u.data_source = b.data_source
     and u.hcc_code = b.hcc_code
),
add_standard_fields as (
    select
          person_id
        , data_source
        , observation_date
        , observation_result
        , condition_code
        , condition_date
        , condition_concept_name
        , hcc_code
        , hcc_description
        , contributing_factor
        , current_year_billed
        , cast('Observation suspect' as {{ dbt.type_string() }}) as reason
        , observation_date as suspect_date
    from add_billed_flag
),
add_data_types as (
    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(observation_date as date) as observation_date
        , cast(observation_result as {{ dbt.type_string() }}) as observation_result
        , cast(condition_code as {{ dbt.type_string() }}) as condition_code
        , cast(condition_date as date) as condition_date
        , cast(condition_concept_name as {{ dbt.type_string() }}) as condition_concept_name
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
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
    , observation_date
    , observation_result
    , condition_code
    , condition_date
    , condition_concept_name
    , hcc_code
    , hcc_description
    , current_year_billed
    , reason
    , contributing_factor
    , suspect_date
from add_data_types

{% endmacro %}