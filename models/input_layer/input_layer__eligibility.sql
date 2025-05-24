{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}
select e.person_id
     , e.member_id
     , e.subscriber_id
     , e.gender
     , e.race
     , e.birth_date
     , e.death_date
     , e.death_flag
     , e.enrollment_start_date
     , e.enrollment_end_date
     , COALESCE(e.payer, '') as payer
     , e.payer_type
     , COALESCE({{ quote_column('plan') }}, '') as {{ quote_column('plan') }}
     , e.original_reason_entitlement_code
     , e.dual_status_code
     , e.medicare_status_code
     , e.group_id
     , e.group_name
     , e.first_name
     , e.last_name
     , e.social_security_number
     , e.subscriber_relation
     , e.address
     , e.city
     , e.state
     , e.zip_code
     , e.phone
     , e.data_source
     , e.file_name
     , e.file_date
     , e.ingest_datetime
from {{ ref('eligibility') }} as e
