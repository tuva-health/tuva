
{{ config(materialized='table', enabled = var('enable_input_layer_testing', true) ) }}

WITH medical_paid_amount_vs_end_date_matrix AS (

    SELECT 'timeliness'                             AS data_quality_category
         , 'medical_paid_amount_vs_end_date_matrix' AS graph_name
         , 'month'                                  AS level_of_detail
         , 'claim_end_date'                         AS y_axis_description
         , 'paid_date'                              AS x_axis_description
         , 'year'                                   AS filter_description
         , 'total_paid_amount'                      AS sum_description
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , SUM(ilmc.paid_amount)                    AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date) )

   , medical_claim_count_vs_end_date_matrix AS (

    SELECT 'timeliness'                             AS data_quality_category
         , 'medical_claim_count_vs_end_date_matrix' AS graph_name
         , 'month'                                  AS level_of_detail
         , 'claim_end_date'                         AS y_axis_description
         , 'paid_date'                              AS x_axis_description
         , 'year'                                   AS filter_description
         , 'unique_number_of_claims'                AS sum_description
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS y_axis
         , DATE_TRUNC('MONTH', ilmc.paid_date)      AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , COUNT(DISTINCT ilmc.claim_id)            AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('MONTH', ilmc.paid_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date) )

   , medical_claim_paid_over_time_monthly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'medical_claim_paid_over_time_monthly'   AS graph_name
         , 'month'                                  AS level_of_detail
         , 'N/A'                             AS y_axis_description
         , 'claim_end_date'                         AS x_axis_description
         , 'N/A'                                    AS filter_description
         , 'total_paid_amount'                                    AS sum_description
         , CAST(NULL AS DATE)                   AS y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , SUM(ilmc.paid_amount)                     AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date) )

   , medical_claim_paid_over_time_yearly AS (
    SELECT 'reasonableness'                        AS data_quality_category
         , 'medical_claim_paid_over_time_yearly'   AS graph_name
         , 'year'                                  AS level_of_detail
         , 'N/A'                            AS y_axis_description
         , 'claim_end_date'                        AS x_axis_description
         , 'N/A'                                   AS filter_description
         , 'total_paid_amount'                                   AS sum_description
         , CAST(NULL AS DATE)                  AS y_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , SUM(ilmc.paid_amount)                   AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY DATE_TRUNC('YEAR', ilmc.claim_end_date) )

   , medical_claim_volume_over_time_monthly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'medical_claim_volume_over_time_monthly' AS graph_name
         , 'month'                                  AS level_of_detail
         , 'N/A'                            AS y_axis_description
         , 'claim_end_date'                         AS x_axis_description
         , 'N/A'                                    AS filter_description
         , 'count_distinct_claim_id'                                    AS sum_description
         , CAST(NULL AS DATE)            AS y_axis
         , DATE_TRUNC('MONTH', ilmc.claim_end_date) AS x_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date)  AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id) AS NUMERIC)                    AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY DATE_TRUNC('MONTH', ilmc.claim_end_date)
           , DATE_TRUNC('YEAR', ilmc.claim_end_date) )

   , medical_claim_volume_over_time_yearly AS (
    SELECT 'reasonableness'                        AS data_quality_category
         , 'medical_claim_volume_over_time_yearly' AS graph_name
         , 'year'                                  AS level_of_detail
         , 'N/A'                           AS y_axis_description
         , 'claim_end_date'                        AS x_axis_description
         , 'N/A'                                   AS filter_description
         , 'count_distinct_claim_id'                                   AS sum_description
         , CAST(NULL AS DATE)          AS y_axis
         , DATE_TRUNC('YEAR', ilmc.claim_end_date) AS x_axis
         , CAST(NULL AS DATE)                      AS chart_filter
         , CAST(COUNT(DISTINCT ilmc.claim_id)  AS NUMERIC)                   AS value

    FROM {{ ref('input_layer__medical_claim') }} AS ilmc

    GROUP BY DATE_TRUNC('YEAR', ilmc.claim_end_date) )


   , pharmacy_paid_amount_vs_dispensing_date_matrix AS (

    SELECT 'timeliness'                                     AS data_quality_category
         , 'pharmacy_paid_amount_vs_dispensing_date_matrix' AS graph_name
         , 'month'                                          AS level_of_detail
         , 'dispensing_date'                                AS y_axis_description
         , 'paid_date'                                      AS x_axis_description
         , 'year'                                           AS filter_description
         , 'total_paid_amount'                              AS sum_description
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         , SUM(ilpc.paid_amount)                            AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date) )

   , pharmacy_claim_count_vs_dispensing_date_matrix AS (

    SELECT 'timeliness'                                     AS data_quality_category
         , 'pharmacy_claim_count_vs_dispensing_date_matrix' AS graph_name
         , 'month'                                          AS level_of_detail
         , 'dispensing_date'                                AS y_axis_description
         , 'paid_date'                                      AS x_axis_description
         , 'year'                                           AS filter_description
         , 'unique_number_of_claims'                        AS sum_description
         , DATE_TRUNC('MONTH', ilpc.dispensing_date)        AS y_axis
         , DATE_TRUNC('MONTH', ilpc.paid_date)              AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)         AS chart_filter
         , COUNT(DISTINCT ilpc.claim_id)                    AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('MONTH', ilpc.paid_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date) )

   , pharmacy_claim_paid_over_time_monthly AS (
    SELECT 'reasonableness'                          AS data_quality_category
         , 'pharmacy_claim_paid_over_time_monthly'   AS graph_name
         , 'month'                                   AS level_of_detail
         , 'total_paid'                              AS y_axis_description
         , 'dispensing_date'                         AS x_axis_description
         , 'N/A'                                     AS filter_description
         , 'n/a'                                     AS sum_description
         , CAST(NULL AS DATE)                   AS y_axis
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         , SUM(ilpc.paid_amount)                   AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date) )

   , pharmacy_claim_paid_over_time_yearly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'pharmacy_claim_paid_over_time_yearly'   AS graph_name
         , 'year'                                   AS level_of_detail
         , 'N/A'                                    AS y_axis_description
         , 'dispensing_date'                        AS x_axis_description
         , 'N/A'                                    AS filter_description
         , 'total_paid'                             AS sum_description
         , CAST(NULL AS DATE)                    AS y_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , SUM(ilpc.paid_amount)                   AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY DATE_TRUNC('YEAR', ilpc.dispensing_date) )

   , pharmacy_claim_volume_over_time_monthly AS (
    SELECT 'reasonableness'                          AS data_quality_category
         , 'pharmacy_claim_volume_over_time_monthly' AS graph_name
         , 'month'                                   AS level_of_detail
         , 'N/A'                             AS y_axis_description
         , 'dispensing_date'                         AS x_axis_description
         , 'N/A'                                     AS filter_description
         , 'count_distinct_claim_id'                                     AS sum_description
         , CAST(NULL AS DATE)                AS y_axis
         , DATE_TRUNC('MONTH', ilpc.dispensing_date) AS x_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date)  AS chart_filter
         , CAST( COUNT(DISTINCT ilpc.claim_id)   AS NUMERIC)                     AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY DATE_TRUNC('MONTH', ilpc.dispensing_date)
           , DATE_TRUNC('YEAR', ilpc.dispensing_date) )

   , pharmacy_claim_volume_over_time_yearly AS (
    SELECT 'reasonableness'                         AS data_quality_category
         , 'pharmacy_claim_volume_over_time_yearly' AS graph_name
         , 'year'                                   AS level_of_detail
         , 'N/A'                            AS y_axis_description
         , 'dispensing_date'                        AS x_axis_description
         , 'N/A'                                    AS filter_description
         , 'count_distinct_claim_id'                  AS sum_description
         , CAST(NULL AS DATE)         AS y_axis
         , DATE_TRUNC('YEAR', ilpc.dispensing_date) AS x_axis
         , CAST(NULL AS DATE)                       AS chart_filter
         , CAST(COUNT(DISTINCT ilpc.claim_id)     AS NUMERIC)                    AS value

    FROM {{ ref('input_layer__pharmacy_claim') }} AS ilpc

    GROUP BY DATE_TRUNC('YEAR', ilpc.dispensing_date) )

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
