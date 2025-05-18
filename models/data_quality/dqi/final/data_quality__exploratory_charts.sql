{{ config(
    materialized='table',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
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
         , cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)   as y_axis
         , cast(DATE_TRUNC(ilmc.paid_date, MONTH) as STRING)        as x_axis
         , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('month', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as y_axis
         , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)         as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         {% endif %}
         , SUM(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.paid_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('month', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
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
         , cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)   as y_axis
         , cast(DATE_TRUNC(ilmc.paid_date, MONTH) as STRING)        as x_axis
         , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('month', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as y_axis
         , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)         as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)      as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         {% endif %}
         , COUNT(distinct ilmc.claim_id) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.paid_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('month', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilmc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)   as x_axis
         , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         {% endif %}
         , SUM(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)   as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)    as x_axis
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         {% endif %}
         , CAST(null as STRING) as chart_filter
         , SUM(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)   as x_axis
         , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)    as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR) as x_axis
         , CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as chart_filter
         , CAST(COUNT(distinct ilmc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_end_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('MONTH', ilmc.claim_end_date) as VARCHAR)
           , CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)   as x_axis
         , cast(NULL as STRING)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(NULL as VARCHAR)             as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR) as x_axis
         , CAST(null as VARCHAR) as chart_filter
         , CAST(COUNT(distinct ilmc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_end_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('year', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('YEAR', ilmc.claim_end_date) as VARCHAR)
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
         , cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)          as y_axis
         , cast(DATE_TRUNC(ilpc.paid_date, MONTH) as STRING)                as x_axis
         , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)           as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(DATE_TRUNC('month', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)           as y_axis
         , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)                 as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)            as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% else %} -- snowflake and redshift
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         {% endif %}
         , SUM(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilpc.paid_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('month', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
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
         , cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)          as y_axis
         , cast(DATE_TRUNC(ilpc.paid_date, MONTH) as STRING)                as x_axis
         , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)           as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(DATE_TRUNC('month', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)           as y_axis
         , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)                 as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)            as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)        as y_axis
         , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)              as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)         as chart_filter
         {% else %} -- snowflake and redshift
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as y_axis
         , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         {% endif %}
         , COUNT(distinct ilpc.claim_id) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilpc.paid_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('month', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('MONTH', ilpc.paid_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)   as x_axis
         , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         {% endif %}
         , SUM(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)   as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)    as x_axis
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         {% endif %}
         , CAST(null as STRING) as chart_filter
         , SUM(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)   as x_axis
         , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)    as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)     as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR) as x_axis
         , CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as chart_filter
         , CAST(COUNT(distinct ilpc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilpc.dispensing_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('MONTH', ilpc.dispensing_date) as VARCHAR)
           , CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
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
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)   as x_axis
         , cast(NULL as STRING)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                      as chart_filter
         , cast(count(distinct ilpc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR) as x_axis
         , CAST(null as VARCHAR) as chart_filter
         , CAST(COUNT(distinct ilpc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilpc.dispensing_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('year', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('YEAR', ilpc.dispensing_date) as VARCHAR)
           {% endif %}
)

, medical_claims_with_eligibility as (
    select 'completeness' as data_quality_category
         , 'medical_claims_with_eligibility' as graph_name
         , 'month' as level_of_detail
         , 'N/A' as y_axis_description
         , 'claim_start_date' as x_axis_description
         , 'claim_year' as filter_description
         , 'percentage_of_claims_with_eligibility' as sum_description
         , CAST(null as STRING) as y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(ilmc.claim_start_date, MONTH) as STRING) as x_axis
         , cast(DATE_TRUNC(ilmc.claim_start_date, YEAR) as STRING) as chart_filter
         , CAST(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('month', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('year', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , CAST(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(DATETRUNC(year, ilmc.claim_start_date) as VARCHAR) as chart_filter
         , cast(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , cast(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as DOUBLE
           ) as value
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('MONTH', ilmc.claim_start_date) as VARCHAR) as x_axis
         , cast(DATE_TRUNC('YEAR', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , cast(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as DECIMAL
           ) as value
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('MONTH', ilmc.claim_start_date) as VARCHAR) as x_axis
         , CAST(DATE_TRUNC('YEAR', ilmc.claim_start_date) as VARCHAR) as chart_filter
         , CAST(
             COUNT(DISTINCT CASE WHEN EXISTS (
                 SELECT 1 FROM {{ ref('input_layer__eligibility') }} ile
                 WHERE ile.person_id = ilmc.person_id
             ) THEN ilmc.claim_id END) * 100.0 /
             NULLIF(COUNT(DISTINCT ilmc.claim_id), 0)
             as NUMERIC
           ) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           cast(DATE_TRUNC(ilmc.claim_start_date, MONTH) as STRING)
           , cast(DATE_TRUNC(ilmc.claim_start_date, YEAR) as STRING)
           {% elif target.type in ('postgres', 'duckdb') %}
           cast(DATE_TRUNC('month', ilmc.claim_start_date) as VARCHAR)
           , cast(DATE_TRUNC('year', ilmc.claim_start_date) as VARCHAR)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_start_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_start_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_start_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_start_date) as VARCHAR)
           {% elif target.type == 'athena' %}
           cast(DATE_TRUNC('MONTH', ilmc.claim_start_date) as VARCHAR)
           , cast(DATE_TRUNC('YEAR', ilmc.claim_start_date) as VARCHAR)
           {% else %} -- snowflake and redshift
           CAST(DATE_TRUNC('MONTH', ilmc.claim_start_date) as VARCHAR)
           , CAST(DATE_TRUNC('YEAR', ilmc.claim_start_date) as VARCHAR)
           {% endif %}
)

, total_paid_yearly as (
    select
        {% if target.type == 'bigquery' %}
        DATE_TRUNC(date_month, YEAR) as year_date,
        {% elif target.type in ('postgres', 'duckdb') %}
        DATE_TRUNC('year', date_month) as year_date,
        {% elif target.type == 'fabric' %}
        DATETRUNC(year, date_month) as year_date,
        {% elif target.type == 'databricks' %}
        DATE_TRUNC('YEAR', date_month) as year_date,
        {% elif target.type == 'athena' %}
        DATE_TRUNC('YEAR', date_month) as year_date,
        {% else %} -- snowflake and redshift
        DATE_TRUNC('YEAR', date_month) as year_date,
        {% endif %}
        SUM(paid_amount) as total_yearly_paid
    from all_claims_monthly
    group by
        {% if target.type == 'bigquery' %}
        DATE_TRUNC(date_month, YEAR)
        {% elif target.type in ('postgres', 'duckdb') %}
        DATE_TRUNC('year', date_month)
        {% elif target.type == 'fabric' %}
        DATETRUNC(year, date_month)
        {% elif target.type == 'databricks' %}
        DATE_TRUNC('YEAR', date_month)
        {% elif target.type == 'athena' %}
        DATE_TRUNC('YEAR', date_month)
        {% else %} -- snowflake and redshift
        DATE_TRUNC('YEAR', date_month)
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
         , cast(DATE_TRUNC(acm.date_month, YEAR) as STRING) as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% endif %}
         , CAST(null as STRING) as chart_filter
         , CAST(SUM(acm.paid_amount) / NULLIF(tpy.total_yearly_paid, 0) * 100 as NUMERIC) as value

    from all_claims_monthly acm
    inner join total_paid_yearly tpy
        on {% if target.type == 'bigquery' %}
           DATE_TRUNC(acm.date_month, YEAR) = tpy.year_date
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', acm.date_month) = tpy.year_date
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, acm.date_month) = tpy.year_date
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% endif %}
    where acm.claim_type = 'professional'
    group by
         y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(acm.date_month, YEAR) as STRING)
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', acm.date_month) as VARCHAR)
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, acm.date_month) as VARCHAR)
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
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
         , cast(DATE_TRUNC(acm.date_month, YEAR) as STRING) as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% endif %}
         , CAST(null as STRING) as chart_filter
         , CAST(SUM(acm.paid_amount) / NULLIF(tpy.total_yearly_paid, 0) * 100 as NUMERIC) as value

    from all_claims_monthly acm
    inner join total_paid_yearly tpy
        on {% if target.type == 'bigquery' %}
           DATE_TRUNC(acm.date_month, YEAR) = tpy.year_date
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', acm.date_month) = tpy.year_date
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, acm.date_month) = tpy.year_date
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% endif %}
    where acm.claim_type = 'institutional'
    group by
         y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(acm.date_month, YEAR) as STRING)
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', acm.date_month) as VARCHAR)
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, acm.date_month) as VARCHAR)
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
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
         , cast(DATE_TRUNC(acm.date_month, YEAR) as STRING) as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR) as x_axis
         {% endif %}
         , CAST(null as STRING) as chart_filter
         , CAST(SUM(acm.paid_amount) / NULLIF(tpy.total_yearly_paid, 0) * 100 as NUMERIC) as value

    from all_claims_monthly acm
    inner join total_paid_yearly tpy
        on {% if target.type == 'bigquery' %}
           DATE_TRUNC(acm.date_month, YEAR) = tpy.year_date
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', acm.date_month) = tpy.year_date
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, acm.date_month) = tpy.year_date
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', acm.date_month) = tpy.year_date
           {% endif %}
    where acm.claim_type = 'pharmacy'
    group by
         y_axis
         {% if target.type == 'bigquery' %}
         , cast(DATE_TRUNC(acm.date_month, YEAR) as STRING)
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(DATE_TRUNC('year', acm.date_month) as VARCHAR)
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, acm.date_month) as VARCHAR)
         {% elif target.type == 'databricks' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
         {% elif target.type == 'athena' %}
         , cast(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
         {% else %} -- snowflake and redshift
         , CAST(DATE_TRUNC('YEAR', acm.date_month) as VARCHAR)
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
