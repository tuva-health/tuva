{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

WITH combined_data_cte AS (
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
    cd.person_id,
    cd.year_nbr,
    cd.year_month,
    cd.member_month_sk,
    cd.data_source,
    cd.patient_source_key,
    cd.payer,
    cd.{{ quote_column('plan') }},
    cd.payer_attributed_provider,
    cd.payer_attributed_provider_practice,
    cd.payer_attributed_provider_organization,
    cd.payer_attributed_provider_lob,
    cd.custom_attributed_provider,
    cd.custom_attributed_provider_practice,
    cd.custom_attributed_provider_organization,
    cd.custom_attributed_provider_lob,
FROM combined_data_cte cd