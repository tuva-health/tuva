with cte as (
select claim_id
 ,claim_line_number
 ,encounter_id
 ,'inpatient hospice' as encounter_type
 ,1 as priority_number
from {{ ref('inpatient_hospice__prof_claims') }}
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

)

select 
  claim_id
, claim_line_number
, encounter_id as old_encounter_id
, row_number() over (order by encounter_type, claim_id) as encounter_id
, encounter_type
, priority_number
, row_number() over (partition by claim_id , claim_line_number order by priority_number) as claim_line_attribution_number
from cte
