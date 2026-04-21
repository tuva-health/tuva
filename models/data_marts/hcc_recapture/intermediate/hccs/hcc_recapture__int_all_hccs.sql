{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

WITH seed_hcc_hierarchy AS (
    SELECT
        model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
    FROM {{ ref('hcc_recapture__stg_hierarchy') }}
),

chronic_hccs AS (
    SELECT
        mpgs.hcc_code
        , mpgs.model_version
        , CASE WHEN mpgs.acute_condition_flag = 'N' THEN 1 ELSE 0 END AS chronic_flag
        , CASE WHEN mpgs.acute_condition_flag = 'Y' THEN 1 ELSE 0 END AS acute_flag
    FROM {{ ref('homeward_chronic_conditions') }} AS mpgs
),

get_risk_code AS (
    SELECT DISTINCT
        person_id
        , payer
        , payment_year
        , model_version
        , risk_model_code
        , ROW_NUMBER() OVER (PARTITION BY person_id, payment_year, model_version ORDER BY collection_end_date DESC) AS month_order
    FROM {{ ref('cms_hcc__int_demographic_factors') }}
    WHERE LOWER(factor_type) = 'demographic'
),

medical_claims AS (
-- Use distinct to remove claim line
    SELECT DISTINCT
        person_id
        , payer
        , claim_id
        , rendering_npi
    FROM {{ ref('core__medical_claim') }}
),

eligible_hccs AS (
    SELECT * FROM {{ ref('hcc_recapture__int_coded_hccs') }}

    UNION ALL

    SELECT * FROM {{ ref('hcc_recapture__int_suspect_hccs') }}
)

-- NOTE: Distinct is to remove different recording dates + ICD 10 codes for the same HCC code
SELECT DISTINCT
    sus.person_id
    , sus.payer
    , sus.data_source
    , {{ date_part('year', 'sus.recorded_date') }} AS collection_year
    , sus.recorded_date
    , sus.model_version
    , sus.claim_id
    , sus.hcc_code
    , sus.hcc_description
    , chronic.chronic_flag AS hcc_chronic_flag
    , COALESCE(hier.hcc_hierarchy_group, 'no hierarchy') AS hcc_hierarchy_group
    , COALESCE(hier.hcc_hierarchy_group_rank, 1) AS hcc_hierarchy_group_rank
    , rcode.risk_model_code
    , CASE WHEN elig_bene.person_id IS NOT NULL THEN 1 ELSE 0 END AS eligible_bene_flag
    , eligible_claim_flag
    , med.rendering_npi
    , suspect_hcc_flag
    , CASE WHEN chronic.chronic_flag = 1 AND eligible_claim_flag = 1 THEN 1 ELSE 0 END AS recapturable_flag
    , hcc_type
    , hcc_source
FROM eligible_hccs AS sus
LEFT JOIN seed_hcc_hierarchy AS hier
    ON
        sus.hcc_code = hier.hcc_code
        AND sus.model_version = hier.model_version
LEFT JOIN chronic_hccs AS chronic
    ON
        sus.model_version = chronic.model_version
        AND sus.hcc_code = chronic.hcc_code
LEFT JOIN get_risk_code AS rcode
    ON
        sus.person_id = rcode.person_id
        AND sus.payer = rcode.payer
        AND {{ date_part('year', 'sus.recorded_date') }} = rcode.payment_year - 1
        AND sus.model_version = rcode.model_version
        AND rcode.month_order = 1
LEFT JOIN medical_claims AS med
    ON
        sus.person_id = med.person_id
        AND sus.payer = med.payer
        AND sus.claim_id = med.claim_id
-- Only include benes eligible for gap closure
LEFT JOIN {{ ref('hcc_recapture__int_eligible_benes') }} AS elig_bene
    ON
        sus.person_id = elig_bene.person_id
        AND {{ date_part('year', 'sus.recorded_date') }} = elig_bene.collection_year
        AND sus.payer = elig_bene.payer
WHERE sus.hcc_code IS NOT NULL
-- Replace with cms_hcc__adjustment_rates once that table includes PY 2026 
AND 1 = (CASE WHEN {{ date_part('year', 'sus.recorded_date') }} >= 2025 AND sus.model_version = 'CMS-HCC-V24' THEN 0 ELSE 1 END)