{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{%- set performance_period_end = var('quality_measures_period_end') -%}

{%- set performance_period_begin -%}
{{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}



select
    cast({{ performance_period_begin }} as date) as performance_period_begin,
    cast('{{ performance_period_end }}' as date) as performance_period_end
