with unique_seq as (
select
	*,
	row_number() over(partition by encounter_id, code_type, diagnosis_code, diagnosis_rank, present_on_admission_code order by 1) as seq
from {{ ref('stg_diagnosis') }}
)

select 
	*
from unique_seq 
where seq > 1