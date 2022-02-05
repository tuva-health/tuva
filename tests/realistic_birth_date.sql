select
	patient_id
,	birth_date 
from core.stg_patient
where date_part(YEAR, cast(birth_date as date)) < 1900

union

select
	patient_id
,	birth_date 
from core.stg_patient
where date_part(YEAR, cast(birth_date as date)) > current_date

union 

select
	patient_id
,	birth_date 
from core.stg_patient
where birth_date > death_date