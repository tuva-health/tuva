
{%- set measure_id -%}
(select id
from {{ ref('quality_measures__measures') }}
where id = 'NQF0059')
{%- endset -%}

{%- set measure_name -%}
(select name
from {{ ref('quality_measures__measures') }}
where id = 'NQF0059')
{%- endset -%}

{%- set measure_version -%}
(select version
from {{ ref('quality_measures__measures') }}
where id = 'NQF0059')
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
    set performance period begin to a year and a day prior
    for a complete calendar year
*/
, period_begin as (

    select
          performance_period_end
        , {{ dbt.dateadd (
              datepart = "day"
            , interval = +1
            , from_date_or_timestamp =
                dbt.dateadd (
                      datepart = "year"
                    , interval = -1
                    , from_date_or_timestamp = "performance_period_end"
            )
          ) }} as performance_period_begin
    from period_end

)

select
      cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
from period_begin