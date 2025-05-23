{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with pharmacy_claim_input as (
    select * from {{ ref('pharmacy_claim') }}
),
product_to_ingredient as (
    select * from {{ ref('pharmacy__product_to_ingredient') }}
),
add_ingredient as (
    select
        pc.person_id,
        -- Get ingredient information to aggregate pdc on active ingredient of ndc
        pti.ingredient_rxcui,
        pti.ingredient_name,
        pc.dispensing_date as drug_exposure_start_date,
        pc.days_supply,
        coalesce(
            -- If days_supply is > 0, add days_supply - 1 since the dispensing date is is day 1
            case
                when pc.days_supply > 0 then pc.dispensing_date + interval '1 day' * (pc.days_supply - 1)
                else null
            end,
            -- Otherwise, make the end date the same as the start date. assumption is that this was a 1 day supply
            pc.dispensing_date
        ) as drug_exposure_end_date
    from pharmacy_claim_input pc
    inner join product_to_ingredient pti
        on pc.ndc_code = pti.ndc
)
select * from add_ingredient
