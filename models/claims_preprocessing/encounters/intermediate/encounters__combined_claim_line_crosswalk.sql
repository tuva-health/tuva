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
)

select 
  claim_id
, claim_line_number
, encounter_id
, encounter_type
, priority_number
, row_number() over (partition by claim_id , claim_line_number order by priority_number) as claim_line_attribution_number
from cte
