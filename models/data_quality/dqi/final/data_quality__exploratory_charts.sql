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
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   as y_axis
         , DATE_TRUNC(ilmc.paid_date, MONTH)        as x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('month', ilmc.paid_date)      as x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as y_axis
         , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)         as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as chart_filter
         {% endif %}
         , SUM(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.paid_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('month', ilmc.paid_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
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
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   as y_axis
         , DATE_TRUNC(ilmc.paid_date, MONTH)        as x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('month', ilmc.paid_date)      as x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as y_axis
         , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)         as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as chart_filter
         {% endif %}
         , COUNT(distinct ilmc.claim_id) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.paid_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('month', ilmc.paid_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(month, ilmc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
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
         , cast(NULL as DATE)                       as y_axis
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   as x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , cast(NULL as DATE)                       as y_axis
         , DATE_TRUNC('month', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(NULL as VARCHAR)                       as y_axis
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , cast(NULL as DATE)                       as y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         {% elif target.type == 'athena' %}
         , cast(NULL as DATE)                       as y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         {% else %} -- snowflake and redshift
         , CAST(null as DATE) as y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as chart_filter
         {% endif %}
         , SUM(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)   as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilmc.claim_end_date) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)    as x_axis
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as x_axis
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as x_axis
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as x_axis
         {% endif %}
         , CAST(null as DATE) as chart_filter
         , SUM(ilmc.paid_amount) as value

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   as x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)     as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  as chart_filter
         , cast(count(distinct ilmc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) as x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as chart_filter
         , CAST(COUNT(distinct ilmc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilmc.claim_end_date) as VARCHAR)
           , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)   as x_axis
         , cast(NULL as DATE)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilmc.claim_end_date) as x_axis
         , cast(NULL as DATE)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)    as x_axis
         , cast(NULL as VARCHAR)             as chart_filter
         , cast(count(distinct ilmc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as x_axis
         , cast(NULL as DATE)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as x_axis
         , cast(NULL as DATE)                      as chart_filter
         , cast(count(distinct ilmc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) as x_axis
         , CAST(null as DATE) as chart_filter
         , CAST(COUNT(distinct ilmc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__medical_claim') }} as ilmc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilmc.claim_end_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
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
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)          as y_axis
         , DATE_TRUNC(ilpc.paid_date, MONTH)                as x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)           as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date)        as y_axis
         , DATE_TRUNC('month', ilpc.paid_date)              as x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)         as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)           as y_axis
         , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)                 as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)            as chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        as y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         as chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        as y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         as chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as chart_filter
         {% endif %}
         , SUM(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.paid_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('month', ilpc.paid_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
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
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)          as y_axis
         , DATE_TRUNC(ilpc.paid_date, MONTH)                as x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)           as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date)        as y_axis
         , DATE_TRUNC('month', ilpc.paid_date)              as x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)         as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)           as y_axis
         , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)                 as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)            as chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        as y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         as chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        as y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         as chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as chart_filter
         {% endif %}
         , COUNT(distinct ilpc.claim_id) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.paid_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('month', ilpc.paid_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(month, ilpc.paid_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)   as x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)    as chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)  as chart_filter
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)     as chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) asx_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  as chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  as chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as chart_filter
         {% endif %}
         , SUM(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)   as x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilpc.dispensing_date) as x_axis
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)    as x_axis
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as x_axis
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as x_axis
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as x_axis
         {% endif %}
         , CAST(null as DATE) as chart_filter
         , SUM(ilpc.paid_amount) as value

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)   as x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)    as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)     as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  as chart_filter
         , cast(count(distinct ilpc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) as x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as chart_filter
         , CAST(COUNT(distinct ilpc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(month, ilpc.dispensing_date) as VARCHAR)
           , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date)
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
         , CAST(null as DATE) as y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)   as x_axis
         , cast(NULL as DATE)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilpc.dispensing_date) as x_axis
         , cast(NULL as DATE)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'fabric' %}
         , cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)    as x_axis
         , cast(NULL as VARCHAR)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as NUMERIC) as value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as x_axis
         , cast(NULL as DATE)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as DOUBLE) as value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as x_axis
         , cast(NULL as DATE)                       as chart_filter
         , cast(count(distinct ilpc.claim_id) as DECIMAL) as value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) as x_axis
         , CAST(null as DATE) as chart_filter
         , CAST(COUNT(distinct ilpc.claim_id) as NUMERIC) as value
         {% endif %}

    from {{ ref('input_layer__pharmacy_claim') }} as ilpc

    group by {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           cast(DATETRUNC(year, ilpc.dispensing_date) as VARCHAR)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% endif %}
)

{% if target.type == 'fabric' %}
select * from medical_paid_amount_vs_end_date_matrix
union
select * from medical_claim_count_vs_end_date_matrix
union
select * from medical_claim_paid_over_time_monthly
union
select * from medical_claim_paid_over_time_yearly
union
select * from medical_claim_volume_over_time_monthly
union
select * from medical_claim_volume_over_time_yearly
union
select * from pharmacy_paid_amount_vs_dispensing_date_matrix
union
select * from pharmacy_claim_count_vs_dispensing_date_matrix
union
select * from pharmacy_claim_paid_over_time_monthly
union
select * from pharmacy_claim_paid_over_time_yearly
union
select * from pharmacy_claim_volume_over_time_monthly
union
select * from pharmacy_claim_volume_over_time_yearly
{% else %}
select * from medical_paid_amount_vs_end_date_matrix
union all
select * from medical_claim_count_vs_end_date_matrix
union all
select * from medical_claim_paid_over_time_monthly
union all
select * from medical_claim_paid_over_time_yearly
union all
select * from medical_claim_volume_over_time_monthly
union all
select * from medical_claim_volume_over_time_yearly
union all
select * from pharmacy_paid_amount_vs_dispensing_date_matrix
union all
select * from pharmacy_claim_count_vs_dispensing_date_matrix
union all
select * from pharmacy_claim_paid_over_time_monthly
union all
select * from pharmacy_claim_paid_over_time_yearly
union all
select * from pharmacy_claim_volume_over_time_monthly
union all
select * from pharmacy_claim_volume_over_time_yearly
{% endif %}
