

-- This dbt model lists all claims with an inpatient bill type.
-- It has these columns:
--    claim_id

-- There is one row per distinct claim_id from
-- the core.medical_claim table
-- that has at least one bill_type_code
-- that starts with '11' or '12'.



with bill_type_requirement as (
select distinct claim_id
from {{ ref('core__medical_claim') }} aa
inner join {{ ref('terminology__bill_type') }} bb
on aa.bill_type_code = bb.bill_type_code
where left(bb.bill_type_code,2) in ('11','12') 
)


select *
from bill_type_requirement
