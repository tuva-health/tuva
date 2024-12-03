{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


select
      eligibility_id
    , person_id
    , subscriber_id
    , birth_date
    , death_date
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ quote_column('plan') }}
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , subscriber_relation
    , data_source
    , tuva_last_run
from {{ ref('core__stg_claims_eligibility') }}