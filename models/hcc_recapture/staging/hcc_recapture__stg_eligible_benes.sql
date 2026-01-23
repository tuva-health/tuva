{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
}}

-- Flattening months to 1 person per year
select distinct
  person_id
  , {{ date_part('year', 'collection_end_date') }} as collection_year
  , payer
from {{ ref('cms_hcc__int_members') }}
-- Don't support ESRD risk scores yet
where enrollment_status != 'ESRD'
