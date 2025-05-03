{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

-- Here we list encounter_ids that meet
-- the time requirement to be an index admission:
-- The discharge date must be at least 30 days
-- earlier than the last discharge date available
-- in the dataset.


with cte as (
  select max(discharge_date) as max_discharge
  from {{ ref('readmissions__encounter') }}
)

select encounter_id
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('readmissions__encounter') }}
cross join cte
where discharge_date <= {{ dbt.dateadd (
datepart = "day"
, interval = -30
, from_date_or_timestamp = "cte.max_discharge"
) }}
