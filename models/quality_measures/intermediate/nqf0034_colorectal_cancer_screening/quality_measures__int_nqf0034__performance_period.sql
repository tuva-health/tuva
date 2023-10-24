{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{%- set performance_period_end = var('quality_measures_period_end') -%}

{%- set performance_period_begin -%}
{{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}

{%- set performance_period_begin_1yp -%}
{{ dbt.dateadd(datepart="year", interval=-2, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}

{%- set performance_period_begin_2yp -%}
{{ dbt.dateadd(datepart="year", interval=-3, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}

{%- set performance_period_begin_4yp -%}
{{ dbt.dateadd(datepart="year", interval=-5, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}

{%- set performance_period_begin_9yp -%}
{{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}



select
    cast({{ performance_period_begin }} as date) as performance_period_begin,
    cast('{{ performance_period_end }}' as date) as performance_period_end,
    cast({{ performance_period_begin_1yp }} as date) as performance_period_begin_1yp,
    cast({{ performance_period_begin_2yp }} as date) as performance_period_begin_2yp,
    cast({{ performance_period_begin_4yp }} as date) as performance_period_begin_4yp,
    cast({{ performance_period_begin_9yp }} as date) as performance_period_begin_9yp
