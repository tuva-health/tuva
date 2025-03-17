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

WITH medical_paid_amount_vs_end_date_matrix AS (

    SELECT 'timeliness'                             AS data_quality_category
         , 'medical_paid_amount_vs_end_date_matrix' AS graph_name
         , 'month'                                  AS level_of_detail
         , 'claim_end_date'                         AS y_axis_description
         , 'paid_date'                              AS x_axis_description
         , 'paid_year'                                   AS filter_description
         , 'total_paid_amount'                      AS sum_description
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   AS y_axis
         , DATE_TRUNC(ilmc.paid_date, MONTH)        AS x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    AS chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('month', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  AS chart_filter
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilmc.claim_end_date)    AS y_axis
         , DATETRUNC(month, ilmc.paid_date)         AS x_axis
         , DATETRUNC(year, ilmc.claim_end_date)     AS chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% endif %}
         , SUM(ilmc.paid_amount)                    AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.paid_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('month', ilmc.paid_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilmc.claim_end_date)
           , DATETRUNC(month, ilmc.paid_date)
           , DATETRUNC(year, ilmc.claim_end_date)
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

   , medical_claim_count_vs_end_date_matrix AS (

    SELECT 'timeliness'                             AS data_quality_category
         , 'medical_claim_count_vs_end_date_matrix' AS graph_name
         , 'month'                                  AS level_of_detail
         , 'claim_end_date'                         AS y_axis_description
         , 'paid_date'                              AS x_axis_description
         , 'paid_year'                                   AS filter_description
         , 'unique_number_of_claims'                AS sum_description
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   AS y_axis
         , DATE_TRUNC(ilmc.paid_date, MONTH)        AS x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    AS chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('month', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  AS chart_filter
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilmc.claim_end_date)    AS y_axis
         , DATETRUNC(month, ilmc.paid_date)         AS x_axis
         , DATETRUNC(year, ilmc.claim_end_date)     AS chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% endif %}
         , COUNT(DISTINCT ilmc.claim_id)            AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.paid_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('month', ilmc.paid_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilmc.claim_end_date)
           , DATETRUNC(month, ilmc.paid_date)
           , DATETRUNC(year, ilmc.claim_end_date)
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

   , medical_claim_paid_over_time_monthly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'medical_claim_paid_over_time_monthly'   AS graph_name
         , 'month'                                  AS level_of_detail
         , 'N/A'                                    AS y_axis_description
         , 'claim_end_date'                         AS x_axis_description
         , 'paid_year'                                    AS filter_description
         , 'total_paid_amount'                      AS sum_description
         {% if target.type == 'bigquery' %}
         , CAST(NULL AS DATE)                       AS y_axis
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   AS x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    AS chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , CAST(NULL AS DATE)                       AS y_axis
         , DATE_TRUNC('month', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  AS chart_filter
         {% elif target.type == 'fabric' %}
         , CAST(NULL AS DATE)                       AS y_axis
         , DATETRUNC(month, ilmc.claim_end_date)    AS x_axis
         , DATETRUNC(year, ilmc.claim_end_date)     AS chart_filter
         {% elif target.type == 'databricks' %}
         , CAST(NULL AS DATE)                       AS y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% elif target.type == 'athena' %}
         , CAST(NULL AS DATE)                       AS y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% else %} -- snowflake and redshift
         , CAST(NULL AS DATE)                       AS y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         {% endif %}
         , SUM(ilmc.paid_amount)                    AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilmc.claim_end_date)
           , DATETRUNC(year, ilmc.claim_end_date)
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

   , medical_claim_paid_over_time_yearly AS (
    SELECT 'reasonableness'                        AS data_quality_category
         , 'medical_claim_paid_over_time_yearly'   AS graph_name
         , 'year'                                  AS level_of_detail
         , 'N/A'                                   AS y_axis_description
         , 'claim_end_date'                        AS x_axis_description
         , 'N/A'                                   AS filter_description
         , 'total_paid_amount'                     AS sum_description
         , CAST(NULL AS DATE)                      AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)   AS x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilmc.claim_end_date) AS x_axis
         {% elif target.type == 'fabric' %}
         , DATETRUNC(year, ilmc.claim_end_date)    AS x_axis
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         {% endif %}
         , CAST(NULL AS DATE)                      AS chart_filter
         , SUM(ilmc.paid_amount)                   AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, ilmc.claim_end_date)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% endif %}
)

   , medical_claim_volume_over_time_monthly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'medical_claim_volume_over_time_monthly' AS graph_name
         , 'month'                                  AS level_of_detail
         , 'N/A'                                    AS y_axis_description
         , 'claim_end_date'                         AS x_axis_description
         , 'paid_year'                              AS filter_description
         , 'count_distinct_claim_id'                AS sum_description
         , CAST(NULL AS DATE)                       AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, MONTH)   AS x_axis
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)    AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('year', ilmc.claim_end_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilmc.claim_end_date)    AS x_axis
         , DATETRUNC(year, ilmc.claim_end_date)     AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS DOUBLE) AS value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS DECIMAL) AS value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% endif %}

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, MONTH)
           , DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilmc.claim_end_date)
           , DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilmc.claim_end_date)
           , DATETRUNC(year, ilmc.claim_end_date)
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

   , medical_claim_volume_over_time_yearly AS (
    SELECT 'reasonableness'                        AS data_quality_category
         , 'medical_claim_volume_over_time_yearly' AS graph_name
         , 'year'                                  AS level_of_detail
         , 'N/A'                                   AS y_axis_description
         , 'claim_end_date'                        AS x_axis_description
         , 'N/A'                                   AS filter_description
         , 'count_distinct_claim_id'               AS sum_description
         , CAST(NULL AS DATE)                      AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilmc.claim_end_date, YEAR)   AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilmc.claim_end_date) AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'fabric' %}
         , DATETRUNC(year, ilmc.claim_end_date)    AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS DOUBLE) AS value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS DECIMAL) AS value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC) AS value
         {% endif %}

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilmc.claim_end_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilmc.claim_end_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, ilmc.claim_end_date)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilmc.claim_end_date)
           {% endif %}
)


   , pharmacy_paid_amount_vs_dispensing_date_matrix AS (

    SELECT 'timeliness'                                     AS data_quality_category
         , 'pharmacy_paid_amount_vs_dispensing_date_matrix' AS graph_name
         , 'month'                                          AS level_of_detail
         , 'dispensing_date'                                AS y_axis_description
         , 'paid_date'                                      AS x_axis_description
         , 'year'                                           AS filter_description
         , 'total_paid_amount'                              AS sum_description
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)          AS y_axis
         , DATE_TRUNC(ilpc.paid_date, MONTH)                AS x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)           AS chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('month', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)         AS chart_filter
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilpc.dispensing_date)           AS y_axis
         , DATETRUNC(month, ilpc.paid_date)                 AS x_axis
         , DATETRUNC(year, ilpc.dispensing_date)            AS chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         {% endif %}
         , SUM(ilpc.paid_amount)                            AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.paid_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('month', ilpc.paid_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilpc.dispensing_date)
           , DATETRUNC(month, ilpc.paid_date)
           , DATETRUNC(year, ilpc.dispensing_date)
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

   , pharmacy_claim_count_vs_dispensing_date_matrix AS (

    SELECT 'timeliness'                                     AS data_quality_category
         , 'pharmacy_claim_count_vs_dispensing_date_matrix' AS graph_name
         , 'month'                                          AS level_of_detail
         , 'dispensing_date'                                AS y_axis_description
         , 'paid_date'                                      AS x_axis_description
         , 'paid_year'                                      AS filter_description
         , 'unique_number_of_claims'                        AS sum_description
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)          AS y_axis
         , DATE_TRUNC(ilpc.paid_date, MONTH)                AS x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)           AS chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('month', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)         AS chart_filter
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilpc.dispensing_date)           AS y_axis
         , DATETRUNC(month, ilpc.paid_date)                 AS x_axis
         , DATETRUNC(year, ilpc.dispensing_date)            AS chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         {% endif %}
         , COUNT(DISTINCT ilpc.claim_id)                    AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.paid_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('month', ilpc.paid_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilpc.dispensing_date)
           , DATETRUNC(month, ilpc.paid_date)
           , DATETRUNC(year, ilpc.dispensing_date)
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

   , pharmacy_claim_paid_over_time_monthly AS (
    SELECT 'reasonableness'                          AS data_quality_category
         , 'pharmacy_claim_paid_over_time_monthly'   AS graph_name
         , 'month'                                   AS level_of_detail
         , 'N/A'                                     AS y_axis_description
         , 'dispensing_date'                         AS x_axis_description
         , 'paid_year'                               AS filter_description
         , 'paid_amount'                             AS sum_description
         , CAST(NULL AS DATE)                        AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)   AS x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)    AS chart_filter
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)  AS chart_filter
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilpc.dispensing_date)    AS x_axis
         , DATETRUNC(year, ilpc.dispensing_date)     AS chart_filter
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         {% endif %}
         , SUM(ilpc.paid_amount)                     AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilpc.dispensing_date)
           , DATETRUNC(year, ilpc.dispensing_date)
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

   , pharmacy_claim_paid_over_time_yearly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'pharmacy_claim_paid_over_time_yearly'   AS graph_name
         , 'year'                                   AS level_of_detail
         , 'N/A'                                    AS y_axis_description
         , 'dispensing_date'                        AS x_axis_description
         , 'N/A'                                    AS filter_description
         , 'total_paid'                             AS sum_description
         , CAST(NULL AS DATE)                       AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)   AS x_axis
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilpc.dispensing_date) AS x_axis
         {% elif target.type == 'fabric' %}
         , DATETRUNC(year, ilpc.dispensing_date)    AS x_axis
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         {% endif %}
         , CAST(NULL AS DATE)                       AS chart_filter
         , SUM(ilpc.paid_amount)                    AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, ilpc.dispensing_date)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% endif %}
)

   , pharmacy_claim_volume_over_time_monthly AS (
    SELECT 'reasonableness'                          AS data_quality_category
         , 'pharmacy_claim_volume_over_time_monthly' AS graph_name
         , 'month'                                   AS level_of_detail
         , 'N/A'                                     AS y_axis_description
         , 'dispensing_date'                         AS x_axis_description
         , 'paid_year'                               AS filter_description
         , 'count_distinct_claim_id'                 AS sum_description
         , CAST(NULL AS DATE)                        AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, MONTH)   AS x_axis
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)    AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('month', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('year', ilpc.dispensing_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'fabric' %}
         , DATETRUNC(month, ilpc.dispensing_date)    AS x_axis
         , DATETRUNC(year, ilpc.dispensing_date)     AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS DOUBLE) AS value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS DECIMAL) AS value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% endif %}

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, MONTH)
           , DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('month', ilpc.dispensing_date)
           , DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(month, ilpc.dispensing_date)
           , DATETRUNC(year, ilpc.dispensing_date)
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

   , pharmacy_claim_volume_over_time_yearly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'pharmacy_claim_volume_over_time_yearly' AS graph_name
         , 'year'                                   AS level_of_detail
         , 'N/A'                                    AS y_axis_description
         , 'dispensing_date'                        AS x_axis_description
         , 'N/A'                                    AS filter_description
         , 'count_distinct_claim_id'                AS sum_description
         , CAST(NULL AS DATE)                       AS y_axis
         {% if target.type == 'bigquery' %}
         , DATE_TRUNC(ilpc.dispensing_date, YEAR)   AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% elif target.type in ('postgres', 'duckdb') %}
         , DATE_TRUNC('year', ilpc.dispensing_date) AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'fabric' %}
         , DATETRUNC(year, ilpc.dispensing_date)    AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% elif target.type == 'databricks' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS DOUBLE) AS value
         {% elif target.type == 'athena' %}
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS DECIMAL) AS value
         {% else %} -- snowflake and redshift
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id) AS NUMERIC) AS value
         {% endif %}

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY {% if target.type == 'bigquery' %}
           DATE_TRUNC(ilpc.dispensing_date, YEAR)
           {% elif target.type in ('postgres', 'duckdb') %}
           DATE_TRUNC('year', ilpc.dispensing_date)
           {% elif target.type == 'fabric' %}
           DATETRUNC(year, ilpc.dispensing_date)
           {% elif target.type == 'databricks' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% elif target.type == 'athena' %}
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% else %} -- snowflake and redshift
           DATE_TRUNC('YEAR', ilpc.dispensing_date)
           {% endif %}
)

{% if target.type == 'fabric' %}
SELECT * FROM medical_paid_amount_vs_end_date_matrix
UNION
SELECT * FROM medical_claim_count_vs_end_date_matrix
UNION
SELECT * FROM medical_claim_paid_over_time_monthly
UNION
SELECT * FROM medical_claim_paid_over_time_yearly
UNION
SELECT * FROM medical_claim_volume_over_time_monthly
UNION
SELECT * FROM medical_claim_volume_over_time_yearly
UNION
SELECT * FROM pharmacy_paid_amount_vs_dispensing_date_matrix
UNION
SELECT * FROM pharmacy_claim_count_vs_dispensing_date_matrix
UNION
SELECT * FROM pharmacy_claim_paid_over_time_monthly
UNION
SELECT * FROM pharmacy_claim_paid_over_time_yearly
UNION
SELECT * FROM pharmacy_claim_volume_over_time_monthly
UNION
SELECT * FROM pharmacy_claim_volume_over_time_yearly
{% else %}
SELECT * FROM medical_paid_amount_vs_end_date_matrix
UNION ALL
SELECT * FROM medical_claim_count_vs_end_date_matrix
UNION ALL
SELECT * FROM medical_claim_paid_over_time_monthly
UNION ALL
SELECT * FROM medical_claim_paid_over_time_yearly
UNION ALL
SELECT * FROM medical_claim_volume_over_time_monthly
UNION ALL
SELECT * FROM medical_claim_volume_over_time_yearly
UNION ALL
SELECT * FROM pharmacy_paid_amount_vs_dispensing_date_matrix
UNION ALL
SELECT * FROM pharmacy_claim_count_vs_dispensing_date_matrix
UNION ALL
SELECT * FROM pharmacy_claim_paid_over_time_monthly
UNION ALL
SELECT * FROM pharmacy_claim_paid_over_time_yearly
UNION ALL
SELECT * FROM pharmacy_claim_volume_over_time_monthly
UNION ALL
SELECT * FROM pharmacy_claim_volume_over_time_yearly
{% endif %}
