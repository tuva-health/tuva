{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',True)))
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
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('eligibility') }}