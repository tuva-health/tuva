{{ config(
     enabled = var('cms_hcc_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      patient_id
    , gender
    , birth_date
    , enrollment_start_date
    , enrollment_end_date
    , dual_status_code
    , medicare_status_code
    , '{{ var('last_update')}}' as last_update
from {{ ref('eligibility') }}