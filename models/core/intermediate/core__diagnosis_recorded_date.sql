
{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
  claim_id,
  data_source,
  coalesce( min(admission_date),
            min(claim_start_date),
	    min(discharge_date),
	    min(claim_end_date)
	  ) as recorded_date
from {{ ref('medical_claim') }}
group by claim_id, data_source
