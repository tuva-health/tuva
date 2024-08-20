with cte as 
(
select claim_id
 ,claim_line_number
 ,encounter_id
 ,'acute inpatient' as encounter_type
 ,0 as priority_number
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'acute inpatient' as encounter_type
,0 as priority_number
from {{ ref('acute_inpatient__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
 ,claim_line_number
 ,encounter_id
 ,'emergency department' as encounter_type
 ,1 as priority_number
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'emergency department' as encounter_type
,1 as priority_number
from {{ ref('emergency_department__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
 ,claim_line_number
 ,encounter_id
 ,'emergency department' as encounter_type
 ,1 as priority_number
from {{ ref('emergency_department__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'inpatient hospice' as encounter_type
,1 as priority_number
from {{ ref('inpatient_hospice__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
,claim_line_number
,encounter_id
,'inpatient psych' as encounter_type
,2 as priority_number
from {{ ref('inpatient_psych__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'inpatient psych' as encounter_type
,2 as priority_number
from {{ ref('inpatient_psych__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
,claim_line_number
,encounter_id
,'inpatient rehabilitation' as encounter_type
,3 as priority_number
from {{ ref('inpatient_rehab__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'inpatient rehabilitation' as encounter_type
,3 as priority_number
from {{ ref('inpatient_rehab__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
,claim_line_number
,encounter_id
,'inpatient skilled nursing' as encounter_type
,4 as priority_number
from {{ ref('inpatient_snf__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'inpatient skilled nursing' as encounter_type
,4 as priority_number
from {{ ref('inpatient_snf__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
,claim_line_number
,encounter_id
,'inpatient substance use' as encounter_type
,5 as priority_number
from {{ ref('inpatient_substance_use__prof_claims') }}
where claim_attribution_number = 1

union 

select enc.claim_id
,med.claim_line_number
,enc.encounter_id
,'inpatient substance use' as encounter_type
,5 as priority_number
from {{ ref('inpatient_substance_use__generate_encounter_id') }} enc
inner join {{ ref('encounters__stg_medical_claim') }} med on enc.claim_id = med.claim_id

union

select claim_id
,claim_line_number
,encounter_id
,'office visit surgery' as encounter_type
,6 as priority_number
from {{ ref('office_visits__int_office_visits_surgery') }}

union

select claim_id
,claim_line_number
,encounter_id
,'office visit injections' as encounter_type
,7 as priority_number
from {{ ref('office_visits__int_office_visits_injections') }}

union

select claim_id
,claim_line_number
,encounter_id
,'office visit PT, OT, ST' as encounter_type
,8 as priority_number
from {{ ref('office_visits__int_office_visits_ptotst') }}

union

select claim_id
,claim_line_number
,encounter_id
,'office visit radiology' as encounter_type
,9 as priority_number
from {{ ref('office_visits__int_office_visits_radiology') }}

union

select claim_id
,claim_line_number
,encounter_id
,'office visits' as encounter_type
,10 as priority_number
from {{ ref('office_visits__int_office_visits') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'urgent care' as encounter_type
,11 as priority_number --urgent care needs to be lower than ed and inpatient
from {{ ref('urgent_care__match_claims_to_anchor') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'outpatient psych' as encounter_type
,12 as priority_number 
from {{ ref('outpatient_psych__match_claims_to_anchor') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'outpatient rehab' as encounter_type
,13 as priority_number 
from {{ ref('outpatient_rehab__match_claims_to_anchor') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'ambulatory surgery center' as encounter_type
,14 as priority_number 
from {{ ref('asc__match_claims_to_anchor') }}

union


select claim_id
,claim_line_number
,old_encounter_id
,'outpatient hospital or clinic' as encounter_type
,999 as priority_number 
from {{ ref('outpatient_hospital_or_clinic__match_claims_to_anchor') }} --lowest outpatient priority, roll up to more specific encounter type when available

union

select claim_id
,claim_line_number
,old_encounter_id
,'dialysis' as encounter_type
,15 as priority_number 
from {{ ref('dialysis__match_claims_to_anchor') }} --should come before generic office and outpatient visit

union

select claim_id
,claim_line_number
,old_encounter_id
,'outpatient hospice' as encounter_type
,16 as priority_number 
from {{ ref('outpatient_hospice__match_claims_to_anchor') }} --should come before generic office and outpatient visit

union

select claim_id
,claim_line_number
,old_encounter_id
,'home health' as encounter_type
,17 as priority_number 
from {{ ref('home_health__match_claims_to_anchor') }} --should come before generic office and outpatient visit

union

select claim_id
,claim_line_number
,old_encounter_id
,'outpatient injections' as encounter_type
,18 as priority_number 
from {{ ref('outpatient_injections__match_claims_to_anchor') }} --should come before generic office and outpatient visit

)

select 
  claim_id
, claim_line_number
, encounter_id as old_encounter_id
, dense_rank() over (order by encounter_type, encounter_id) as encounter_id
, encounter_type
, priority_number
, row_number() over (partition by claim_id , claim_line_number order by priority_number) as claim_line_attribution_number
from cte
