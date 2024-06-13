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
    set performance period begin to January or October
    for visits in influenza season
*/
, period_begin as (

    select
          performance_period_end
        , case
          when extract(month from performance_period_end) between 1 and 3
            then extract(year from performance_period_end) || '-01-01'
          else extract(year from performance_period_end) || '-10-01'
          end
          as performance_period_begin
    from period_end

)

-- lookback_period for august
, lookback_period as (

  select
      *
    , case
        when extract(month from performance_period_end) between 1 and 3
            then (extract(year from performance_period_end) - 1) || '-08-01'
        when extract(month from performance_period_end) between 10 and 12
            then extract(year from performance_period_end) || '-08-01'
        else NULL
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
