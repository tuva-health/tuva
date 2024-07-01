WITH cte AS (
    SELECT 
        payment_year,
        patient_id,
        model_version,
        patient_risk_sk,
        SUM(coefficient) AS risk_score
    FROM {{ ref('mart_review__patient_risk_factors') }}
    GROUP BY payment_year,
             patient_id,
             model_version,
             patient_risk_sk
)

SELECT 
    CASE 
        WHEN risk_score <= 0.5 THEN '.5'
        WHEN risk_score BETWEEN 0.5 AND 1.0 THEN '1'
        WHEN risk_score BETWEEN 1.0 AND 1.5 THEN '1.5'
        WHEN risk_score BETWEEN 1.5 AND 2.0 THEN '2'
        WHEN risk_score BETWEEN 2.0 AND 2.5 THEN '2.5'
        WHEN risk_score BETWEEN 2.5 AND 3.0 THEN '3'
        WHEN risk_score BETWEEN 3.0 AND 3.5 THEN '3.5'
        WHEN risk_score BETWEEN 3.5 AND 4.0 THEN '4'
        WHEN risk_score BETWEEN 4.0 AND 4.5 THEN '4.5'
        WHEN risk_score BETWEEN 4.5 AND 5.0 THEN '5'
        WHEN risk_score > 5.0 THEN '5+'
        ELSE null 
        END AS risk_score_bucket,
        payment_year,
        patient_id,
        model_version,
        patient_risk_sk,
        risk_score
FROM cte
