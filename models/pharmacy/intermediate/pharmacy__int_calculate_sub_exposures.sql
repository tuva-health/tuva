{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with drug_exposure_input as (
   select * from {{ ref('pharmacy__stg_add_ingredient_concepts') }}
),
-- Get the previous end date for each drug exposure to identify gaps between drug exposures
lagged_data as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       -- Set the end date of the previous exposure for the person and ingredient
       lag(drug_exposure_end_date) over(
           partition by person_id, ingredient_rxcui 
           order by drug_exposure_start_date, drug_exposure_end_date
       ) as prev_end_date
   from drug_exposure_input
),
-- Assign a group id to each continuous exposure period. Exposures with gaps greater than 1 day get a new group id
exposures_with_groups as (
   select
       person_id,
       ingredient_rxcui,
       ingredient_name,
       drug_exposure_start_date,
       drug_exposure_end_date,
       -- Use a running sum to create group ids. Increment the group id when there's a gap > 1 day between exposures
       sum(case
           when drug_exposure_start_date > coalesce(
               prev_end_date + interval '1 day', 
               drug_exposure_start_date - interval '1 day'
           ) 
           then 1 else 0 
       end) over(
           partition by person_id, ingredient_rxcui
           order by drug_exposure_start_date, drug_exposure_end_date
           rows between unbounded preceding and current row
       ) as group_id
   from lagged_data
)
select
   person_id,
   ingredient_rxcui,
   ingredient_name,
   min(drug_exposure_start_date) as drug_sub_exposure_start_date,
   max(drug_exposure_end_date) as drug_sub_exposure_end_date,
   count(*) as drug_exposure_count,
   datediff('day', min(drug_exposure_start_date), max(drug_exposure_end_date)) + 1 as days_exposed
from exposures_with_groups
group by
   person_id,
   ingredient_rxcui,
   ingredient_name,
   group_id
