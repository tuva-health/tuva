{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
 | as_bool
   )
}}

{%- set measure_id -%}
(select id
from {{ ref('quality_measures__measures') }}
where id = 'NQF0041')
{%- endset -%}

{%- set measure_name -%}
(select name
from {{ ref('quality_measures__measures') }}
where id = 'NQF0041')
{%- endset -%}

{%- set measure_version -%}
(select version
from {{ ref('quality_measures__measures') }}
where id = 'NQF0041')
{%- endset -%}

/*
    set performance period end to the end of the current calendar year
    or use the quality_measures_period_end variable if provided
      - set quality_measures_period_end to december end for last quarter measurement period
      - set quality_measures_period_end to march end for first quarter measurement period     
*/

with period_end as (

    select
        {% if var('quality_measures_period_end',False) == False -%}
        {{ last_day(dbt.current_timestamp(), 'year') }}
        {% else -%}
        cast('{{ var('quality_measures_period_end') }}' as date)
        {%- endif %}
         as performance_period_end
         
)

/*
    set performance period begin to following day of 3 months prior
    for visits in influenza season
*/
, period_begin as (

    select
          performance_period_end
        , {{ dbt.dateadd (
              datepart = "day"
            , interval = +1
            , from_date_or_timestamp =
                dbt.dateadd (
                      datepart = "month"
                    , interval = -3
                    , from_date_or_timestamp = "performance_period_end"
            )
          ) }} as performance_period_begin
    from period_end

)

/*
    lookback_period for august of either current or previous year
    for immunization qualifying date
*/
, lookback_period as (

  select
      *
        , case
            when {{ date_part('month', 'performance_period_end') | as_number }} between 1 and 8
            then {{ dbt.concat([
                "cast(" ~
                date_part('year', 'performance_period_end') ~
                " as integer) - 1",
                "'-08-01'"
            ]) }}
            else {{ dbt.concat([
                date_part('year', 'performance_period_end'),
                "'-08-01'"
            ]) }}
        end as lookback_period_august
  from period_begin

)

select
      cast({{ measure_id }} as {{ dbt.type_string() }}) as measure_id
    , cast({{ measure_name }} as {{ dbt.type_string() }}) as measure_name
    , cast({{ measure_version }} as {{ dbt.type_string() }}) as measure_version
    , cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
    , cast(lookback_period_august as date) as lookback_period_august
from lookback_period
