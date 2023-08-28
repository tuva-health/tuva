{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- Here we list encounter_ids that meet
-- the time requirement to be an index admission:
-- The discharge date must be at least 30 days
-- earlier than the last discharge date available
-- in the dataset.



select encounter_id, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('readmissions__encounter') }}
where discharge_date <= (select max(discharge_date)
                         from {{ ref('readmissions__encounter') }} ) - 30

