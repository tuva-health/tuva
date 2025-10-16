{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

-- This model now receives claim lines that are already adjusted at the claim level
-- with ingredient-based therapy keys ensuring brand/generic continuity
-- We just need to aggregate by ingredient and calculate sub-exposures

with adjusted_claim_lines as (
    select * from {{ ref('pharmacy__stg_add_ingredient_concepts') }}
),

-- Aggregate consecutive sub-exposures for the same ingredient
-- Since adjustments were done at claim level with ingredient-based keys, we can just aggregate by ingredient
ingredient_sub_exposures as (
    select
        person_id,
        ingredient_rxcui,
        ingredient_name,
        drug_exposure_start_date as drug_sub_exposure_start_date,
        drug_exposure_end_date as drug_sub_exposure_end_date,
        count(*) as drug_exposure_count,
        {{ dbt.datediff(
            'drug_exposure_start_date',
            'drug_exposure_end_date',
            'day'
        ) }} + 1 as days_exposed
    from adjusted_claim_lines
    group by
        person_id,
        ingredient_rxcui,
        ingredient_name,
        drug_exposure_start_date,
        drug_exposure_end_date
)

-- Final output with sub-exposures ready for PDC calculation
select
    person_id,
    ingredient_rxcui,
    ingredient_name,
    drug_sub_exposure_start_date,
    drug_sub_exposure_end_date,
    drug_exposure_count,
    days_exposed
from ingredient_sub_exposures
