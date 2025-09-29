{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

WITH monthly_patient_risk_cte AS (
    SELECT
       TO_CHAR(collection_end_date, 'YYYYMM') AS year_month,
       person_id,
       normalized_risk_score
    FROM {{ ref('cms_hcc__patient_risk_scores_monthly') }}
),

monthly_population_risk_cte AS (
    SELECT
       TO_CHAR(collection_end_date, 'YYYYMM') AS year_month,
       AVG(normalized_risk_score) AS monthly_avg_risk_score
    FROM {{ ref('cms_hcc__patient_risk_scores_monthly') }}
    GROUP BY
        TO_CHAR(collection_end_date, 'YYYYMM')
),
combined_data_cte AS (
    SELECT
        mm.person_id,
        mm.data_source,
        {{ dbt.concat(["mm.person_id", "'|'", "mm.data_source"]) }} AS patient_source_key,
        mm.year_month,
        mm.payer,
        mm.{{ quote_column('plan') }},
        1 AS member_months_value,
        mpr.normalized_risk_score,
        CASE
            WHEN pop_risk.monthly_avg_risk_score IS NOT NULL AND pop_risk.monthly_avg_risk_score != 0
            THEN mpr.normalized_risk_score / pop_risk.monthly_avg_risk_score
            ELSE NULL
        END AS population_normalized_risk_score,
        LEFT(mm.year_month, 4) AS year_nbr
    FROM {{ ref('core__member_months') }} mm
    LEFT JOIN monthly_patient_risk_cte mpr
        ON mm.person_id = mpr.person_id AND mm.year_month = mpr.year_month
    LEFT JOIN monthly_population_risk_cte pop_risk
        ON mm.year_month = pop_risk.year_month
)
SELECT
    cd.person_id,
    cd.year_nbr,
    cd.year_month,
    cd.member_months_value AS member_months,
    SUM(cd.member_months_value) OVER (PARTITION BY cd.person_id, cd.year_nbr) AS total_year_months,
    CASE
      WHEN SUM(cd.member_months_value) OVER (PARTITION BY cd.person_id, cd.year_nbr) > 0
      THEN CAST(cd.member_months_value AS DECIMAL(10,4)) / SUM(cd.member_months_value) OVER (PARTITION BY cd.person_id, cd.year_nbr)
      ELSE 0
    END AS MonthAllocationFactor,
    cd.data_source,
    cd.patient_source_key,
    cd.payer,
    cd.{{ quote_column('plan') }},
    cd.normalized_risk_score,
    cd.population_normalized_risk_score
FROM combined_data_cte cd