

-- This dbt model lists all claims with at least one DRG code.
-- It has these columns:
--   claim_id

-- There is one row per distinct claim_id from the core.medical_claim
-- table that has at least one valid MS-DRG code (from terminology) or at
-- least one valid APR-DRG code (from terminology).



with drg_requirement as (
select distinct claim_id
from {{ ref('core__medical_claim') }} mc

left join {{ ref('terminology__ms_drg') }} msdrg
on mc.ms_drg_code = msdrg.ms_drg_code

left join {{ ref('terminology__apr_drg') }} aprdrg
on mc.apr_drg_code = aprdrg.apr_drg_code

where (msdrg.ms_drg_code is not null) 
   or (aprdrg.apr_drg_code is not null)
)



select *
from drg_requirement
