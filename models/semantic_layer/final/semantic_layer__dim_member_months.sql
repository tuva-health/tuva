{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

WITH data_with_sks AS (
    SELECT
        mm.person_id,
        mm.data_source,
        {{ dbt.concat(["mm.person_id", "'|'", "mm.data_source"]) }} AS patient_source_key,
        {{ dbt.concat(["mm.person_id", "'|'", "mm.year_month"]) }} as member_month_sk,
        mm.year_month,
        mm.payer,
        mm.{{ quote_column('plan') }},
        mm.payer_attributed_provider,
        mm.payer_attributed_provider_practice,
        mm.payer_attributed_provider_organization,
        mm.payer_attributed_provider_lob,
        mm.custom_attributed_provider,
        mm.custom_attributed_provider_practice,
        mm.custom_attributed_provider_organization,
        mm.custom_attributed_provider_lob,
        LEFT(mm.year_month, 4) AS year_nbr
    FROM {{ ref('core__member_months') }} mm
)
SELECT
    dws.person_id,
    dws.year_nbr,
    dws.year_month,
    dws.member_month_sk,
    dws.data_source,
    dws.patient_source_key,
    dws.payer,
    dws.{{ quote_column('plan') }},
    dws.payer_attributed_provider,
    dws.payer_attributed_provider_practice,
    dws.payer_attributed_provider_organization,
    dws.payer_attributed_provider_lob,
    dws.custom_attributed_provider,
    dws.custom_attributed_provider_practice,
    dws.custom_attributed_provider_organization,
    dws.custom_attributed_provider_lob,
FROM data_with_sks dws