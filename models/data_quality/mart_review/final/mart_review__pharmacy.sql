WITH dedup_prac AS (
    SELECT DISTINCT practitioner_id,
                    provider_first_name,
                    provider_last_name,
                    specialty
    FROM {{ ref('core__practitioner')}}
),
dedup_loc AS (
    SELECT DISTINCT location_id,
                    npi,
                    name
    FROM {{ ref('core__location')}}
)

SELECT 
    p.claim_id,
    p.claim_line_number,
    p.patient_id,
    p.data_source,
    CONCAT(p.patient_id, '|', p.data_source) AS patient_source_key,
    p.ndc_code,
    COALESCE(n.fda_description, n.rxnorm_description) AS ndc_description,
    p.paid_amount,
    p.allowed_amount,
    p.prescribing_provider_id,
    p.prescribing_provider_name,
    prac.specialty AS prescribing_specialty,
    p.dispensing_provider_id,
    p.dispensing_provider_name,
    p.paid_date,
    p.dispensing_date,
    p.days_supply,
    n.rxcui,
    n.rxnorm_description,
    r.brand_name,
    r.brand_vs_generic,
    r.ingredient_name,
    a.atc_1_name,
    a.atc_2_name,
    a.atc_3_name,
    a.atc_4_name
FROM {{ ref('core__pharmacy_claim')}} p
LEFT JOIN {{ ref('terminology__ndc')}} n ON p.ndc_code = n.ndc
LEFT JOIN {{ ref('terminology__rxnorm_brand_generic') }} r ON n.rxcui = r.product_rxcui
LEFT JOIN {{ ref('terminology__rxnorm_to_atc') }} a ON n.rxcui = a.rxcui
LEFT JOIN dedup_prac prac ON p.prescribing_provider_id = prac.practitioner_id
LEFT JOIN dedup_loc l ON p.dispensing_provider_id = l.location_id
