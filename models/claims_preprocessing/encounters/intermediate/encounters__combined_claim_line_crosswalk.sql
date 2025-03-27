{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with cte as (
select claim_id
 , claim_line_number
 , encounter_id
 , 'acute inpatient' as encounter_type
 , 'inpatient' as encounter_group
 , 0 as priority_number
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'acute inpatient' as encounter_type
, 'inpatient' as encounter_group
, 0 as priority_number
from {{ ref('acute_inpatient__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
 , claim_line_number
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , 1 as priority_number
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'emergency department' as encounter_type
, 'outpatient' as encounter_group
, 1 as priority_number
from {{ ref('emergency_department__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
 , claim_line_number
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , 1 as priority_number
from {{ ref('emergency_department__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'inpatient hospice' as encounter_type
, 'inpatient' as encounter_group
, 1 as priority_number
from {{ ref('inpatient_hospice__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, 2 as priority_number
from {{ ref('inpatient_psych__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, 2 as priority_number
from {{ ref('inpatient_psych__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, 3 as priority_number
from {{ ref('inpatient_rehab__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, 3 as priority_number
from {{ ref('inpatient_rehab__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, 4 as priority_number
from {{ ref('inpatient_snf__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, 4 as priority_number
from {{ ref('inpatient_snf__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
, claim_line_number
, encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, 5 as priority_number
from {{ ref('inpatient_substance_use__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, enc.encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, 5 as priority_number
from {{ ref('inpatient_substance_use__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med on enc.claim_id = med.claim_id

union all

select claim_id
, claim_line_number
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, 9 as priority_number --priority set in combined office visit encounter ranking model
from {{ ref('office_visits__int_office_visits_claim_line') }}
where encounter_type = 'office visit radiology'


union all

select claim_id
, claim_line_number
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, 10 as priority_number --priority set in combined office visit encounter ranking model
from {{ ref('office_visits__int_office_visits_claim_line') }}
where encounter_type <> 'office visit radiology'


union all

select claim_id
, claim_line_number
, old_encounter_id
, 'urgent care' as encounter_type
, 'outpatient' as encounter_group
, 11 as priority_number --urgent care needs to be lower than ed and inpatient
from {{ ref('urgent_care__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient psych' as encounter_type
, 'outpatient' as encounter_group
, 12 as priority_number
from {{ ref('outpatient_psych__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient rehabilitation' as encounter_type
, 'outpatient' as encounter_group
, 13 as priority_number
from {{ ref('outpatient_rehab__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'ambulatory surgery center' as encounter_type
, 'outpatient' as encounter_group
, 14 as priority_number
from {{ ref('asc__match_claims_to_anchor') }}

union all


select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient hospital or clinic' as encounter_type
, 'outpatient' as encounter_group
, 999 as priority_number
from {{ ref('outpatient_hospital_or_clinic__match_claims_to_anchor') }} --lowest outpatient priority, roll up to more specific encounter type when available

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient surgery' as encounter_type
, 'outpatient' as encounter_group
, 18 as priority_number
from {{ ref('outpatient_surgery__match_claims_to_anchor') }}


union all

select claim_id
, claim_line_number
, old_encounter_id
, 'dialysis' as encounter_type
, 'outpatient' as encounter_group
, 15 as priority_number
from {{ ref('dialysis__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient hospice' as encounter_type
, 'outpatient' as encounter_group
, 16 as priority_number
from {{ ref('outpatient_hospice__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'home health' as encounter_type
, 'outpatient' as encounter_group
, 17 as priority_number
from {{ ref('home_health__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient injections' as encounter_type
, 'outpatient' as encounter_group
, 19 as priority_number
from {{ ref('outpatient_injections__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient pt/ot/st' as encounter_type
, 'outpatient' as encounter_group
, 20 as priority_number
from {{ ref('outpatient_ptotst__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient substance use' as encounter_type
, 'outpatient' as encounter_group
, 21 as priority_number
from {{ ref('outpatient_substance_use__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'outpatient radiology' as encounter_type
, 'outpatient' as encounter_group
, 22 as priority_number
from {{ ref('outpatient_radiology__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'lab - orphaned' as encounter_type
, 'other' as encounter_group
, 1000000 as priority_number
from {{ ref('lab__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'dme - orphaned' as encounter_type
, 'other' as encounter_group
, 1000001 as priority_number
from {{ ref('dme__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, old_encounter_id
, 'ambulance - orphaned' as encounter_type
, 'other' as encounter_group
, 1000002 as priority_number
from {{ ref('ambulance__match_claims_to_anchor') }}

)


select
  claim_id
, claim_line_number
, encounter_id as old_encounter_id
, dense_rank() over (
order by encounter_type, encounter_id) as encounter_id
, encounter_type
, encounter_group
, priority_number
, row_number() over (partition by claim_id, claim_line_number
order by priority_number) as claim_line_attribution_number
from cte
