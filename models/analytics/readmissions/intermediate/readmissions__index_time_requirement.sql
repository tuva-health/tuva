
-- Here we list encounter_ids that meet
-- the time requirement to be an index admission:
-- The discharge date must be at least 30 days
-- earlier than the last discharge date available
-- in the dataset.


{{ config(enabled=var('readmissions_enabled',var('tuva_packages_enabled',True))) }}




select encounter_id
from {{ ref('readmissions__stg_encounter') }}
where discharge_date <= (select max(discharge_date)
                         from {{ ref('readmissions__stg_encounter') }} ) - 30

