{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with sub_exposures as (
    select * from {{ ref('pharmacy__int_calculate_sub_exposures') }}
),

-- Apply a 30-day persistence window
-- Sub-exposures within 30 days are combined into the same era
get_end_dates as (
    select
        person_id,
        ingredient_rxcui,
        ingredient_name,
        event_date as end_date
    from (
        select
            person_id,
            ingredient_rxcui,
            ingredient_name,
            event_date,
            event_type,
            max(start_ordinal) over (
                partition by person_id, ingredient_rxcui
                order by event_date, event_type
                rows unbounded preceding
            ) as start_ordinal,
            row_number() over (
                partition by person_id, ingredient_rxcui
                order by event_date, event_type
            ) as overall_ord
        from (
            -- Start events for sub-exposures
            select
                person_id,
                ingredient_rxcui,
                ingredient_name,
                drug_sub_exposure_start_date as event_date,
                -1 as event_type,
                row_number() over (
                    partition by person_id, ingredient_rxcui
                    order by drug_sub_exposure_start_date
                ) as start_ordinal
            from sub_exposures

            union all

            -- End events padded by 30 days for the grace period
            -- This +30 days creates the persistence window
            select
                person_id,
                ingredient_rxcui,
                ingredient_name,
                {{ dbt.dateadd('day', 30, 'drug_sub_exposure_end_date') }} as event_date,
                1 as event_type,
                null as start_ordinal
            from sub_exposures
        ) raw_data
    ) e
    -- Event matching algorithm: pairs start/end events to identify era boundaries
    where (2 * e.start_ordinal) - e.overall_ord = 0
),

-- Determine the final drug era end date by joining sub-exposures with padded end dates
drug_era_ends as (
    select
        se.person_id,
        se.ingredient_rxcui,
        min(se.ingredient_name) as ingredient_name,
        se.drug_sub_exposure_start_date,
        min(e.end_date) as drug_era_end_date, 
        se.drug_exposure_count,
        se.days_exposed
    from sub_exposures se
    inner join get_end_dates e
        on se.person_id = e.person_id
        and se.ingredient_rxcui = e.ingredient_rxcui
        and e.end_date >= se.drug_sub_exposure_start_date
    group by
        se.person_id,
        se.ingredient_rxcui,
        se.drug_sub_exposure_start_date,
        se.drug_exposure_count,
        se.days_exposed
),

-- Aggregate results
final_eras as (
    select
        person_id,
        ingredient_rxcui,
        min(ingredient_name) as ingredient_name,
        min(drug_sub_exposure_start_date) as drug_era_start_date,
        drug_era_end_date,
        sum(drug_exposure_count) as drug_exposure_count,
        greatest(0, {{ dbt.datediff('min(drug_sub_exposure_start_date)', 'drug_era_end_date', 'day') }} + 1 - sum(days_exposed)) as gap_days,
        sum(days_exposed) as total_days_exposed,
        {{ dbt.datediff('min(drug_sub_exposure_start_date)', 'drug_era_end_date', 'day') }} + 1 as era_duration_in_days
    from drug_era_ends
    group by
        person_id,
        ingredient_rxcui,
        drug_era_end_date
)

-- Calculate PDC and select final columns
select
    person_id,
    ingredient_rxcui,
    ingredient_name,
    cast(drug_era_start_date as date) as drug_era_start_date,
    cast(drug_era_end_date as date) as drug_era_end_date,
    cast(drug_exposure_count as integer) as drug_exposure_count,
    cast(total_days_exposed as integer) as total_days_exposed,
    cast(era_duration_in_days as integer) as era_duration_in_days,
    cast(gap_days as integer) as gap_days,
    -- Safety guard for division by zero
    round(
        case 
            when era_duration_in_days > 0 
            then (cast(total_days_exposed as {{ dbt.type_float() }}) / cast(era_duration_in_days as {{ dbt.type_float() }})) * 100
            else 0 
        end, 
        2
    ) as pdc,
    '{{ var('tuva_last_run') }}' as tuva_last_run
from final_eras
