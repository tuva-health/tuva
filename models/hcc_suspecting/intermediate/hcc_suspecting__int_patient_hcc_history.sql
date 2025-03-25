{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with all_conditions as (

    select
          person_id
        , data_source
        , recorded_date
        , condition_type
        , icd_10_cm_code
        , hcc_code
        , hcc_description
    from {{ ref('hcc_suspecting__int_all_conditions') }}
    where hcc_code is not null

)

, hcc_grouped as (

    select
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , min(recorded_date) as first_recorded
        , max(recorded_date) as last_recorded
    from all_conditions
    where hcc_code is not null
    group by
          person_id
        , hcc_code
        , hcc_description
        , data_source

)

, hcc_billed as (

    select
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , max(recorded_date) as last_billed
    from all_conditions
    where hcc_code is not null
    and lower(condition_type) <> 'problem'
    group by
          person_id
        , hcc_code
        , hcc_description
        , data_source

)

, add_flag as (

    select
          hcc_grouped.person_id
        , hcc_grouped.data_source
        , hcc_grouped.hcc_code
        , hcc_grouped.hcc_description
        , hcc_grouped.first_recorded
        , hcc_grouped.last_recorded
        , hcc_billed.last_billed
        , case
            when {{ date_part('year', 'hcc_billed.last_billed') }} = {{ date_part('year', dbt.current_timestamp()) }}
            then 1
            else 0
          end as current_year_billed

    from hcc_grouped
         left join hcc_billed
         on hcc_grouped.person_id = hcc_billed.person_id
         and hcc_grouped.hcc_code = hcc_billed.hcc_code
         and hcc_grouped.data_source = hcc_billed.data_source

)

, all_conditions_with_flag as (

    select distinct
          all_conditions.person_id
        , all_conditions.data_source
        , all_conditions.recorded_date
        , all_conditions.condition_type
        , all_conditions.icd_10_cm_code
        , all_conditions.hcc_code
        , all_conditions.hcc_description
        , add_flag.first_recorded
        , add_flag.last_recorded
        , add_flag.last_billed
        , add_flag.current_year_billed
        , cast('Prior coding history' as {{ dbt.type_string() }}) as reason
        , {{ concat_custom([
            "icd_10_cm_code",
            "case"
            " when last_billed is not null then " ~
            concat_custom(["' last billed on '", "last_billed"]) ~
            " when last_billed is null and last_recorded is not null then " ~
            concat_custom(["' last recorded on '", "last_recorded"]) ~
            " else ' (missing recorded and billing dates) '"
            " end"
        ]) }} as contributing_factor
        , coalesce(last_billed, last_recorded) as condition_date
    from all_conditions
         left join add_flag
            on all_conditions.person_id = add_flag.person_id
            and all_conditions.hcc_code = add_flag.hcc_code
            and all_conditions.data_source = add_flag.data_source

)

, add_standard_fields as (

    select distinct
          person_id
        , data_source
        , recorded_date
        , condition_type
        , icd_10_cm_code
        , hcc_code
        , hcc_description
        , first_recorded
        , last_recorded
        , last_billed
        , current_year_billed
        , 'Prior coding history' as reason
        , {{ concat_custom([
            "icd_10_cm_code",
            "case"
            " when last_billed is not null then " ~
            concat_custom(["' last billed on '", "last_billed"]) ~
            " when last_billed is null and last_recorded is not null then " ~
            concat_custom(["' last recorded on '", "last_recorded"]) ~
            " else ' (missing recorded and billing dates) '"
            " end"
        ]) }} as contributing_factor
        , coalesce(last_billed, last_recorded) as suspect_date
    from all_conditions_with_flag

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as {{ dbt.type_string() }}) as condition_type
        , cast(icd_10_cm_code as {{ dbt.type_string() }}) as icd_10_cm_code
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(first_recorded as date) as first_recorded
        , cast(last_recorded as date) as last_recorded
        , cast(last_billed as date) as last_billed
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
    , recorded_date
    , condition_type
    , icd_10_cm_code
    , hcc_code
    , hcc_description
    , first_recorded
    , last_recorded
    , last_billed
    , current_year_billed
    , reason
    , contributing_factor
    , suspect_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types