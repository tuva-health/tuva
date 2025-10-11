{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with drug_exposure_input as (
   select * from {{ ref('pharmacy__stg_add_ingredient_concepts') }}
),

--{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with drug_exposure_input as (
   select * from {{ ref('pharmacy__stg_add_ingredient_concepts') }}
),

-- Adjust overlapping fills to start after previous supply ends (PQA requirement)
adjusted_exposures as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       days_supply,
       -- Get the end date of the previous exposure for this person and ingredient
       lag(drug_exposure_end_date) over(
           partition by person_id, ingredient_rxcui 
           order by drug_exposure_start_date, drug_exposure_end_date
       ) as prev_end_date,
       -- Calculate adjusted start and end dates
       case
           -- If current fill starts before or on the day previous fill ends (overlap scenario)
           when drug_exposure_start_date <= coalesce(
               lag(drug_exposure_end_date) over(
                   partition by person_id, ingredient_rxcui 
                   order by drug_exposure_start_date, drug_exposure_end_date
               ),
               drug_exposure_start_date - interval '1 day'  -- First fill, no adjustment needed
           )
           -- Adjust start date to be day after previous supply ends
           then coalesce(
               lag(drug_exposure_end_date) over(
                   partition by person_id, ingredient_rxcui 
                   order by drug_exposure_start_date, drug_exposure_end_date
               ),
               drug_exposure_start_date - interval '1 day'
           ) + interval '1 day'
           -- No overlap, use actual start date
           else drug_exposure_start_date
       end as adjusted_start_date
   from drug_exposure_input
),

-- Calculate adjusted end dates based on days_supply from adjusted start date
recalculated_dates as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       adjusted_start_date as drug_exposure_start_date,
       -- Recalculate end date: adjusted_start + days_supply - 1
       case
           when days_supply > 0 then adjusted_start_date + interval '1 day' * (days_supply - 1)
           else adjusted_start_date
       end as drug_exposure_end_date,
       -- Get previous adjusted end date for gap calculation
       lag(
           case
               when days_supply > 0 then adjusted_start_date + interval '1 day' * (days_supply - 1)
               else adjusted_start_date
           end
       ) over(
           partition by person_id, ingredient_rxcui 
           order by adjusted_start_date
       ) as prev_adjusted_end_date
   from adjusted_exposures
),

-- Assign group IDs to continuous exposure periods (gaps > 1 day get new group)
exposures_with_groups as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       -- Create group IDs based on gaps after adjustment
       sum(case
           when drug_exposure_start_date > coalesce(
               prev_adjusted_end_date + interval '1 day', 
               drug_exposure_start_date - interval '1 day'
           ) 
           then 1 else 0 
       end) over(
           partition by person_id, ingredient_rxcui
           order by drug_exposure_start_date, drug_exposure_end_date
           rows between unbounded preceding and current row
       ) as group_id
   from recalculated_dates
)

-- Aggregate by group to create sub-exposures
select
   person_id,
   ingredient_rxcui,
   ingredient_name,
   min(drug_exposure_start_date) as drug_sub_exposure_start_date,
   max(drug_exposure_end_date) as drug_sub_exposure_end_date,
   count(*) as drug_exposure_count,
   -- days_exposed represents sum of adjusted supplies in the sub-exposure
   datediff('day', min(drug_exposure_start_date), max(drug_exposure_end_date)) + 1 as days_exposed
from exposures_with_groups
group by
   person_id,
   ingredient_rxcui,
   ingredient_name,
   group_idAdjust overlapping fills to start after previous supply ends (PQA requirement)
adjusted_exposures as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       days_supply,
       -- Get the end date of the previous exposure for this person and ingredient
       lag(drug_exposure_end_date) over(
           partition by person_id, ingredient_rxcui 
           order by drug_exposure_start_date, drug_exposure_end_date
       ) as prev_end_date,
       -- Calculate adjusted start and end dates
       case
           -- If current fill starts before or on the day previous fill ends (overlap scenario)
           when drug_exposure_start_date <= coalesce(
               lag(drug_exposure_end_date) over(
                   partition by person_id, ingredient_rxcui 
                   order by drug_exposure_start_date, drug_exposure_end_date
               ),
               drug_exposure_start_date - interval '1 day'  -- First fill, no adjustment needed
           )
           -- Adjust start date to be day after previous supply ends
           then coalesce(
               lag(drug_exposure_end_date) over(
                   partition by person_id, ingredient_rxcui 
                   order by drug_exposure_start_date, drug_exposure_end_date
               ),
               drug_exposure_start_date - interval '1 day'
           ) + interval '1 day'
           -- No overlap, use actual start date
           else drug_exposure_start_date
       end as adjusted_start_date
   from drug_exposure_input
),

-- Calculate adjusted end dates based on days_supply from adjusted start date
recalculated_dates as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       adjusted_start_date as drug_exposure_start_date,
       -- Recalculate end date: adjusted_start + days_supply - 1
       case
           when days_supply > 0 then adjusted_start_date + interval '1 day' * (days_supply - 1)
           else adjusted_start_date
       end as drug_exposure_end_date,
       -- Get previous adjusted end date for gap calculation
       lag(
           case
               when days_supply > 0 then adjusted_start_date + interval '1 day' * (days_supply - 1)
               else adjusted_start_date
           end
       ) over(
           partition by person_id, ingredient_rxcui 
           order by adjusted_start_date
       ) as prev_adjusted_end_date
   from adjusted_exposures
),

-- Assign group IDs to continuous exposure periods (gaps > 1 day get new group)
exposures_with_groups as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       -- Create group IDs based on gaps after adjustment
       sum(case
           when drug_exposure_start_date > coalesce(
               prev_adjusted_end_date + interval '1 day', 
               drug_exposure_start_date - interval '1 day'
           ) 
           then 1 else 0 
       end) over(
           partition by person_id, ingredient_rxcui
           order by drug_exposure_start_date, drug_exposure_end_date
           rows between unbounded preceding and current row
       ) as group_id
   from recalculated_dates
)

-- Aggregate by group to create sub-exposures
select
   person_id,
   ingredient_rxcui,
   ingredient_name,
   min(drug_exposure_start_date) as drug_sub_exposure_start_date,
   max(drug_exposure_end_date) as drug_sub_exposure_end_date,
   count(*) as drug_exposure_count,
   -- days_exposed represents sum of adjusted supplies in the sub-exposure
   datediff('day', min(drug_exposure_start_date), max(drug_exposure_end_date)) + 1 as days_exposed
from exposures_with_groups
group by
   person_id,
   ingredient_rxcui,
   ingredient_name,
   group_id
