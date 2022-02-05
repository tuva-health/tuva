select
	patient_id
,	death_date 
from {{ ref('stg_patient') }}
where date_part(YEAR, cast(death_date as date)) > current_date

union 

select
	patient_id
,	death_date 
from {{ ref('stg_patient') }}
where birth_date > death_date