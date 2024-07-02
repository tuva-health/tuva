WITH medical_claim AS (
    SELECT
        data_source,
        patient_id,
        year_month,
        CAST(SUM(paid_amount) AS DECIMAL(18,2)) AS paid_amount
    FROM {{ ref('mart_review__stg_medical_claim') }}
    GROUP BY data_source
    , patient_id
    , year_month
)

,pharmacy_claim AS (
    SELECT
        data_source,
        patient_id,
        year_month,
        CAST(SUM(paid_amount) AS DECIMAL(18,2)) AS paid_amount
    FROM {{ ref('mart_review__stg_pharmacy_claim') }}
    GROUP BY data_source
    , patient_id
    , year_month
)

SELECT 
    mm.data_source,
    mm.year_month,
    SUM(CASE WHEN mc.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS members_with_medical_claims,
    SUM(CASE WHEN pc.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS members_with_pharmacy_claims,
    SUM(CASE WHEN pc.patient_id IS NOT NULL THEN 1 
             WHEN mc.patient_id is not null THEN 1 ELSE 0 END) AS members_with_claims,
    COUNT(*) AS total_member_months,
    CAST(SUM(CASE WHEN pc.patient_id IS NOT NULL THEN 1 
             WHEN mc.patient_id is not null THEN 1 ELSE 0 END) / cast(COUNT(*) AS DECIMAL(18,2)) as decimal (18,2)) AS percent_members_with_claims,
    CAST(SUM(CASE WHEN mc.patient_id IS NOT NULL THEN 1 ELSE 0 END) / cast(COUNT(*) AS DECIMAL(18,2)) as decimal (18,2)) AS percent_members_with_medical_claims,
    CAST(SUM(CASE WHEN pc.patient_id IS NOT NULL THEN 1 ELSE 0 END) / cast(COUNT(*) AS DECIMAL(18,2)) as decimal (18,2)) AS percent_members_with_pharmacy_claims
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('core__member_months')}} mm
LEFT JOIN medical_claim mc 
    ON mm.patient_id = mc.patient_id
    AND mm.data_source = mc.data_source
    AND mm.year_month = mc.year_month
LEFT JOIN pharmacy_claim pc
    ON mm.patient_id = pc.patient_id
    AND mm.data_source = pc.data_source
    AND mm.year_month = pc.year_month
GROUP BY mm.data_source, mm.year_month

