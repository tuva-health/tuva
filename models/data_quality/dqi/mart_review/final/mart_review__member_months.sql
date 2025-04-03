{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

SELECT m.*,
    COALESCE(p.total_paid, 0) AS total_paid,
    COALESCE(p.medical_paid, 0) AS medical_paid,
    COALESCE(p.pharmacy_paid, 0) AS pharmacy_paid,
    {{ concat_custom([
        'm.person_id',
        "'|'",
        'm.data_source'
    ]) }} AS patient_data_source_key
FROM {{ ref('core__member_months')}} m
LEFT JOIN {{ ref('financial_pmpm__pmpm_prep') }} p ON m.person_id = p.person_id
    AND m.data_source = p.data_source
    AND m.year_month = p.year_month
    AND m.payer = p.payer
    AND m.{{ quote_column('plan') }} = p.{{ quote_column('plan') }}
