with unique_seq as (
select
	*,
	row_number() over(partition by patient_id, gender, birth_date, death_date order by 1) as seq
from {{ ref('stg_patient') }}
)

select 
	*
from unique_seq 
where seq > 1