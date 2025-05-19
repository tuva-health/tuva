{{ config(
    materialized='table',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    )
    and
    (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

with medical_paid_amount_vs_end_date_matrix as (

    select 'timeliness' as data_quality_category
         , 'medical_paid_amount_vs_end_date_matrix' as graph_name
         , 'month' as level_of_detail
         , 'claim_end_date' as y_axis_description
         , 'paid_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'total_paid_amount' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)   as y_axis
         , cast(date_trunc(ilmc.paid_date, MONTH) as STRING)        as x_axis
         , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('month', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)    as y_axis
         , cast(datetrunc(month, ilmc.paid_date) as VARCHAR)         as x_axis
         , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         {% endif %}
         , sum(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.paid_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('month', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)
           , cast(datetrunc(month, ilmc.paid_date) as VARCHAR)
           , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% endif %}
)

   , medical_claim_count_vs_end_date_matrix as (

    select 'timeliness' as data_quality_category
         , 'medical_claim_count_vs_end_date_matrix' as graph_name
         , 'month' as level_of_detail
         , 'claim_end_date' as y_axis_description
         , 'paid_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'unique_number_of_claims' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)   as y_axis
         , cast(date_trunc(ilmc.paid_date, MONTH) as STRING)        as x_axis
         , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('month', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)    as y_axis
         , cast(datetrunc(month, ilmc.paid_date) as VARCHAR)         as x_axis
         , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         {% endif %}
         , count(distinct ilmc.claim_id) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.paid_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('month', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)
           , cast(datetrunc(month, ilmc.paid_date) as VARCHAR)
           , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% endif %}
)

   , medical_claim_paid_over_time_monthly as (
    select 'reasonableness' as data_quality_category
         , 'medical_claim_paid_over_time_monthly' as graph_name
         , 'month' as level_of_detail
         , 'N/A' as y_axis_description
         , 'claim_end_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'total_paid_amount' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)   as x_axis
         , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         {% endif %}
         , sum(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)
           , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% endif %}
)

   , medical_claim_paid_over_time_yearly as (
    select 'reasonableness' as data_quality_category
         , 'medical_claim_paid_over_time_yearly' as graph_name
         , 'year' as level_of_detail
         , 'N/A' as y_axis_description
         , 'claim_end_date' as x_axis_description
         , 'N/A' as filter_description
         , 'total_paid_amount' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)   as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)    as x_axis
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as chart_filter
         {% else %}
         , cast(null as VARCHAR) as chart_filter
         {% endif %}
         , sum(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% endif %}
)

   , medical_claim_volume_over_time_monthly as (
    select 'reasonableness' as data_quality_category
         , 'medical_claim_volume_over_time_monthly' as graph_name
         , 'month' as level_of_detail
         , 'N/A' as y_axis_description
         , 'claim_end_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'count_distinct_claim_id' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)   as x_axis
         , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_end_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilmc.claim_end_date) as VARCHAR)
           , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% endif %}
)

   , medical_claim_volume_over_time_yearly as (
    select 'reasonableness' as data_quality_category
         , 'medical_claim_volume_over_time_yearly' as graph_name
         , 'year' as level_of_detail
         , 'N/A' as y_axis_description
         , 'claim_end_date' as x_axis_description
         , 'N/A' as filter_description
         , 'count_distinct_claim_id' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)   as x_axis
         , cast(NULL as STRING)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(NULL as VARCHAR)             as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(null as VARCHAR) as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% endif %}
)


   , pharmacy_paid_amount_vs_dispensing_date_matrix as (

    select 'timeliness' as data_quality_category
         , 'pharmacy_paid_amount_vs_dispensing_date_matrix' as graph_name
         , 'month' as level_of_detail
         , 'dispensing_date' as y_axis_description
         , 'paid_date' as x_axis_description
         , 'year' as filter_description
         , 'total_paid_amount' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)          as y_axis
         , cast(date_trunc(ilpc.paid_date, MONTH) as STRING)                as x_axis
         , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)           as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(date_trunc('month', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)           as y_axis
         , cast(datetrunc(month, ilpc.paid_date) as VARCHAR)                 as x_axis
         , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)            as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         {% endif %}
         , sum(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)
           , cast(date_trunc(ilpc.paid_date, MONTH) as STRING)
           , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('month', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)
           , cast(datetrunc(month, ilpc.paid_date) as VARCHAR)
           , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

   , pharmacy_claim_count_vs_dispensing_date_matrix as (

    select 'timeliness' as data_quality_category
         , 'pharmacy_claim_count_vs_dispensing_date_matrix' as graph_name
         , 'month' as level_of_detail
         , 'dispensing_date' as y_axis_description
         , 'paid_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'unique_number_of_claims' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)          as y_axis
         , cast(date_trunc(ilpc.paid_date, MONTH) as STRING)                as x_axis
         , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)           as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(date_trunc('month', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)           as y_axis
         , cast(datetrunc(month, ilpc.paid_date) as VARCHAR)                 as x_axis
         , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)            as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as y_axis
         , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         {% endif %}
         , count(distinct ilpc.claim_id) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)
           , cast(date_trunc(ilpc.paid_date, MONTH) as STRING)
           , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('month', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)
           , cast(datetrunc(month, ilpc.paid_date) as VARCHAR)
           , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

   , pharmacy_claim_paid_over_time_monthly as (
    select 'reasonableness' as data_quality_category
         , 'pharmacy_claim_paid_over_time_monthly' as graph_name
         , 'month' as level_of_detail
         , 'N/A' as y_axis_description
         , 'dispensing_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'paid_amount' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)   as x_axis
         , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         {% endif %}
         , sum(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)
           , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)
           , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

   , pharmacy_claim_paid_over_time_yearly as (
    select 'reasonableness' as data_quality_category
         , 'pharmacy_claim_paid_over_time_yearly' as graph_name
         , 'year' as level_of_detail
         , 'N/A' as y_axis_description
         , 'dispensing_date' as x_axis_description
         , 'N/A' as filter_description
         , 'total_paid' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)   as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)    as x_axis
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as chart_filter
         {% else %}
         , cast(null as VARCHAR) as chart_filter
         {% endif %}
         , sum(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

   , pharmacy_claim_volume_over_time_monthly as (
    select 'reasonableness' as data_quality_category
         , 'pharmacy_claim_volume_over_time_monthly' as graph_name
         , 'month' as level_of_detail
         , 'N/A' as y_axis_description
         , 'dispensing_date' as x_axis_description
         , 'paid_year' as filter_description
         , 'count_distinct_claim_id' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)   as x_axis
         , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)    as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)     as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilpc.dispensing_date, MONTH) as STRING)
           , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(month, ilpc.dispensing_date) as VARCHAR)
           , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

   , pharmacy_claim_volume_over_time_yearly as (
    select 'reasonableness' as data_quality_category
         , 'pharmacy_claim_volume_over_time_yearly' as graph_name
         , 'year' as level_of_detail
         , 'N/A' as y_axis_description
         , 'dispensing_date' as x_axis_description
         , 'N/A' as filter_description
         , 'count_distinct_claim_id' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)   as x_axis
         , cast(NULL as STRING)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilpc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(null as VARCHAR) as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(datetrunc(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

{% if target.type == 'fabric' %}
, medical_claims_with_eligibility as (
    select
        'completeness' as data_quality_category,
        'medical_claims_with_eligibility' as graph_name,
        'month' as level_of_detail,
        'N/A' as y_axis_description,
        'claim_start_date' as x_axis_description,
        'claim_year' as filter_description,
        'percentage_of_claims_with_eligibility' as sum_description,
        cast(null as VARCHAR) as y_axis,
        total.claim_month as x_axis,
        total.claim_year as chart_filter,
        cast(coalesce(with_elig.claims_with_elig, 0) * 100.0 /
            nullif(total.total_claims, 0) as NUMERIC) as value
    from (
        select
            cast(datetrunc(month, claim_start_date) as VARCHAR) as claim_month,
            cast(datetrunc(year, claim_start_date) as VARCHAR) as claim_year,
            count(distinct claim_id) as total_claims
        from {{ ref('input_layer__medical_claim') }}
        group by
            cast(datetrunc(month, claim_start_date) as VARCHAR),
            cast(datetrunc(year, claim_start_date) as VARCHAR)
    ) as total
    left join (
        select
            cast(datetrunc(month, mc.claim_start_date) as VARCHAR) as claim_month,
            cast(datetrunc(year, mc.claim_start_date) as VARCHAR) as claim_year,
            count(distinct mc.claim_id) as claims_with_elig
        from {{ ref('input_layer__medical_claim') }} as mc
        inner join {{ ref('input_layer__eligibility') }} as e
            on mc.person_id = e.person_id
        group by
            cast(datetrunc(month, mc.claim_start_date) as VARCHAR),
            cast(datetrunc(year, mc.claim_start_date) as VARCHAR)
    ) as with_elig
    on total.claim_month = with_elig.claim_month
    and total.claim_year = with_elig.claim_year
)
{% else %}
, medical_claims_with_eligibility as (
    select 'completeness' as data_quality_category
         , 'medical_claims_with_eligibility' as graph_name
         , 'month' as level_of_detail
         , 'N/A' as y_axis_description
         , 'claim_start_date' as x_axis_description
         , 'claim_year' as filter_description
         , 'percentage_of_claims_with_eligibility' as sum_description
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as y_axis
         {% else %}
         , cast(null as VARCHAR) as y_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(ilmc.claim_start_date, MONTH) as STRING) as x_axis
         , cast(date_trunc(ilmc.claim_start_date, YEAR) as STRING) as chart_filter
         , CAST(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('month', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(date_trunc('year', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , CAST(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('MONTH', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , cast(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as DOUBLE
           ) as value
         {% elif target.type == 'athena' %}
         , cast(date_trunc('MONTH', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , cast(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as DECIMAL
           ) as value
         {% else %} -- snowflake and redshift
         , cast(date_trunc('MONTH', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(date_trunc('YEAR', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , cast(
             count(distinct case when exists (
                 select 1 from {{ ref('input_layer__eligibility') }} as ile
                 where ile.person_id = ilmc.person_id
             ) then ilmc.claim_id end) * 100.0 /
             nullif(count(distinct ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(date_trunc(ilmc.claim_start_date, MONTH) as STRING)
           , cast(date_trunc(ilmc.claim_start_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(date_trunc('month', ilmc.claim_start_date) as VARCHAR)
           , cast(date_trunc('year', ilmc.claim_start_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(date_trunc('MONTH', ilmc.claim_start_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_start_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(date_trunc('MONTH', ilmc.claim_start_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_start_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(date_trunc('MONTH', ilmc.claim_start_date) as VARCHAR)
           , cast(date_trunc('YEAR', ilmc.claim_start_date) as VARCHAR)
           {% endif %}
)
{% endif %}

, medical_claims_monthly as (
    select
        coalesce(claim_type, 'unknown') as claim_type
        {% if target.type == 'bigquery' %}
        , date_trunc(claim_start_date, MONTH) as date_month
        {% elif target.type in ('postgres', 'duckdb') %}
        , date_trunc('month', claim_start_date) as date_month
        {% elif target.type == 'fabric' %}
        , datetrunc(month, claim_start_date) as date_month
        {% elif target.type == 'databricks' %}
        , date_trunc('MONTH', claim_start_date) as date_month
        {% elif target.type == 'athena' %}
        , date_trunc('MONTH', claim_start_date) as date_month
        {% else %} -- snowflake and redshift
        , date_trunc('MONTH', claim_start_date) as date_month
        {% endif %}
        , sum(paid_amount) as paid_amount
    from {{ ref('input_layer__medical_claim') }}
    where claim_start_date is not null
    group by
        coalesce(claim_type, 'unknown')
        {% if target.type == 'bigquery' %}
        , date_trunc(claim_start_date, MONTH)
        {% elif target.type in ('postgres', 'duckdb') %}
        , date_trunc('month', claim_start_date)
        {% elif target.type == 'fabric' %}
        , datetrunc(month, claim_start_date)
        {% elif target.type == 'databricks' %}
        , date_trunc('MONTH', claim_start_date)
        {% elif target.type == 'athena' %}
        , date_trunc('MONTH', claim_start_date)
        {% else %} -- snowflake and redshift
        , date_trunc('MONTH', claim_start_date)
        {% endif %}
)

, pharmacy_claims_monthly as (
    select
        'pharmacy' as claim_type
        {% if target.type == 'bigquery' %}
        , date_trunc(dispensing_date, MONTH) as date_month
        {% elif target.type in ('postgres', 'duckdb') %}
        , date_trunc('month', dispensing_date) as date_month
        {% elif target.type == 'fabric' %}
        , datetrunc(month, dispensing_date) as date_month
        {% elif target.type == 'databricks' %}
        , date_trunc('MONTH', dispensing_date) as date_month
        {% elif target.type == 'athena' %}
        , date_trunc('MONTH', dispensing_date) as date_month
        {% else %} -- snowflake and redshift
        , date_trunc('MONTH', dispensing_date) as date_month
        {% endif %}
        , sum(paid_amount) as paid_amount
    from {{ ref('input_layer__pharmacy_claim') }}
    where dispensing_date is not null
    group by
        {% if target.type == 'bigquery' %}
        date_trunc(dispensing_date, MONTH)
        {% elif target.type in ('postgres', 'duckdb') %}
        date_trunc('month', dispensing_date)
        {% elif target.type == 'fabric' %}
        datetrunc(month, dispensing_date)
        {% elif target.type == 'databricks' %}
        date_trunc('MONTH', dispensing_date)
        {% elif target.type == 'athena' %}
        date_trunc('MONTH', dispensing_date)
        {% else %} -- snowflake and redshift
        date_trunc('MONTH', dispensing_date)
        {% endif %}
)

, all_claims_monthly as (
    select claim_type
           , date_month
           , paid_amount
    from medical_claims_monthly
    union all
    select claim_type
           , date_month
           , paid_amount
    from pharmacy_claims_monthly
)

, total_paid_yearly as (
    select
        {% if target.type == 'bigquery' %}
        date_trunc(date_month, YEAR) as year_date
        {% elif target.type in ('postgres', 'duckdb') %}
        date_trunc('year', date_month) as year_date
        {% elif target.type == 'fabric' %}
        datetrunc(year, date_month) as year_date
        {% elif target.type == 'databricks' %}
        date_trunc('YEAR', date_month) as year_date
        {% elif target.type == 'athena' %}
        date_trunc('YEAR', date_month) as year_date
        {% else %} -- snowflake and redshift
        date_trunc('YEAR', date_month) as year_date
        {% endif %}
        , sum(paid_amount) as total_yearly_paid
    from all_claims_monthly
    group by
        {% if target.type == 'bigquery' %}
        date_trunc(date_month, YEAR)
        {% elif target.type in ('postgres', 'duckdb') %}
        date_trunc('year', date_month)
        {% elif target.type == 'fabric' %}
        datetrunc(year, date_month)
        {% elif target.type == 'databricks' %}
        date_trunc('YEAR', date_month)
        {% elif target.type == 'athena' %}
        date_trunc('YEAR', date_month)
        {% else %} -- snowflake and redshift
        date_trunc('YEAR', date_month)
        {% endif %}
)

, professional_claims_yearly_pct as (
    select 'reasonableness' as data_quality_category
         , 'professional_claims_yearly_percentage' as graph_name
         , 'year' as level_of_detail
         , 'professional (benchmark: 20%-40%)' as y_axis_description
         , 'claim_year' as x_axis_description
         , 'N/A' as filter_description
         , 'percent_of_total_spend' as sum_description
         , 'professional' as y_axis
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(acm.date_month, YEAR) as STRING) as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as chart_filter
         {% else %}
         , cast(null as VARCHAR) as chart_filter
         {% endif %}
         , cast(sum(acm.paid_amount) / nullif(tpy.total_yearly_paid, 0) * 100 as NUMERIC) as value

    from all_claims_monthly as acm
    inner join total_paid_yearly as tpy
        on {% if target.type == 'bigquery' %}
           date_trunc(acm.date_month, YEAR) = tpy.year_date
           {% elif target.type in ('postgres', 'duckdb') %}
           date_trunc('year', acm.date_month) = tpy.year_date
           {% elif target.type == 'fabric' %}
           datetrunc(year, acm.date_month) = tpy.year_date
           {% elif target.type == 'databricks' %}
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% elif target.type == 'athena' %}
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% else %} -- snowflake and redshift
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% endif %}
    where acm.claim_type = 'professional'
    group by
         {% if target.type == 'bigquery' %}
         cast(date_trunc(acm.date_month, YEAR) as STRING)
         {% elif target.type in ('postgres', 'duckdb') %}
         cast(date_trunc('year', acm.date_month) as VARCHAR)
         {% elif target.type == 'fabric' %}
         cast(datetrunc(year, acm.date_month) as VARCHAR)
         {% elif target.type == 'databricks' %}
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% elif target.type == 'athena' %}
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% else %} -- snowflake and redshift
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% endif %}
         , tpy.total_yearly_paid
)

, institutional_claims_yearly_pct as (
    select 'reasonableness' as data_quality_category
         , 'institutional_claims_yearly_percentage' as graph_name
         , 'year' as level_of_detail
         , 'institutional (benchmark: 50%-80%)' as y_axis_description
         , 'claim_year' as x_axis_description
         , 'N/A' as filter_description
         , 'percent_of_total_spend' as sum_description
         , 'institutional' as y_axis
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(acm.date_month, YEAR) as STRING) as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as chart_filter
         {% else %}
         , cast(null as VARCHAR) as chart_filter
         {% endif %}
         , cast(sum(acm.paid_amount) / nullif(tpy.total_yearly_paid, 0) * 100 as NUMERIC) as value

    from all_claims_monthly as acm
    inner join total_paid_yearly as tpy
        on {% if target.type == 'bigquery' %}
           date_trunc(acm.date_month, YEAR) = tpy.year_date
           {% elif target.type in ('postgres', 'duckdb') %}
           date_trunc('year', acm.date_month) = tpy.year_date
           {% elif target.type == 'fabric' %}
           datetrunc(year, acm.date_month) = tpy.year_date
           {% elif target.type == 'databricks' %}
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% elif target.type == 'athena' %}
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% else %} -- snowflake and redshift
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% endif %}
    where acm.claim_type = 'institutional'
    group by
         {% if target.type == 'bigquery' %}
         cast(date_trunc(acm.date_month, YEAR) as STRING)
         {% elif target.type in ('postgres', 'duckdb') %}
         cast(date_trunc('year', acm.date_month) as VARCHAR)
         {% elif target.type == 'fabric' %}
         cast(datetrunc(year, acm.date_month) as VARCHAR)
         {% elif target.type == 'databricks' %}
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% elif target.type == 'athena' %}
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% else %} -- snowflake and redshift
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% endif %}
         , tpy.total_yearly_paid
)

, pharmacy_claims_yearly_pct as (
    select 'reasonableness' as data_quality_category
         , 'pharmacy_claims_yearly_percentage' as graph_name
         , 'year' as level_of_detail
         , 'pharmacy (benchmark: 10%-30%)' as y_axis_description
         , 'claim_year' as x_axis_description
         , 'N/A' as filter_description
         , 'percent_of_total_spend' as sum_description
         , 'pharmacy' as y_axis
         {% if target.type == 'bigquery' %}
         , cast(date_trunc(acm.date_month, YEAR) as STRING) as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(date_trunc('year', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(datetrunc(year, acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'databricks' %}
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , cast(date_trunc('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% endif %}
         {% if target.type == 'bigquery' %}
         , cast(null as STRING) as chart_filter
         {% else %}
         , cast(null as VARCHAR) as chart_filter
         {% endif %}
         , cast(sum(acm.paid_amount) / nullif(tpy.total_yearly_paid, 0) * 100 as NUMERIC) as value

    from all_claims_monthly as acm
    inner join total_paid_yearly as tpy
        on {% if target.type == 'bigquery' %}
           date_trunc(acm.date_month, YEAR) = tpy.year_date
           {% elif target.type in ('postgres', 'duckdb') %}
           date_trunc('year', acm.date_month) = tpy.year_date
           {% elif target.type == 'fabric' %}
           datetrunc(year, acm.date_month) = tpy.year_date
           {% elif target.type == 'databricks' %}
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% elif target.type == 'athena' %}
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% else %} -- snowflake and redshift
           date_trunc('YEAR', acm.date_month) = tpy.year_date
           {% endif %}
    where acm.claim_type = 'pharmacy'
    group by
         {% if target.type == 'bigquery' %}
         cast(date_trunc(acm.date_month, YEAR) as STRING)
         {% elif target.type in ('postgres', 'duckdb') %}
         cast(date_trunc('year', acm.date_month) as VARCHAR)
         {% elif target.type == 'fabric' %}
         cast(datetrunc(year, acm.date_month) as VARCHAR)
         {% elif target.type == 'databricks' %}
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% elif target.type == 'athena' %}
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% else %} -- snowflake and redshift
         cast(date_trunc('YEAR', acm.date_month) as VARCHAR)
         {% endif %}
         , tpy.total_yearly_paid
)



{% if target.type == 'fabric' %}
select * from medical_paid_amount_vs_end_date_matrix
union
select * from medical_claim_count_vs_end_date_matrix
union
select * from medical_claim_paid_over_time_yearly
union
select * from medical_claim_volume_over_time_yearly
union
select * from pharmacy_paid_amount_vs_dispensing_date_matrix
union
select * from pharmacy_claim_count_vs_dispensing_date_matrix
union
select * from pharmacy_claim_paid_over_time_yearly
union
select * from pharmacy_claim_volume_over_time_yearly
union
select * from medical_claims_with_eligibility
union
select * from professional_claims_yearly_pct
union
select * from institutional_claims_yearly_pct
union
select * from pharmacy_claims_yearly_pct
{% else %}
select * from medical_paid_amount_vs_end_date_matrix
union all
select * from medical_claim_count_vs_end_date_matrix
union all
select * from medical_claim_paid_over_time_yearly
union all
select * from medical_claim_volume_over_time_yearly
union all
select * from pharmacy_paid_amount_vs_dispensing_date_matrix
union all
select * from pharmacy_claim_count_vs_dispensing_date_matrix
union all
select * from pharmacy_claim_paid_over_time_yearly
union all
select * from pharmacy_claim_volume_over_time_yearly
union all
select * from medical_claims_with_eligibility
union all
select * from professional_claims_yearly_pct
union all
select * from institutional_claims_yearly_pct
union all
select * from pharmacy_claims_yearly_pct
{% endif %}
