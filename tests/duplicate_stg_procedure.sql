with unique_seq as (
select
	*,
	row_number() over(partition by encounter_id, code_type, procedure_code order by 1) as seq
from {{ ref('stg_procedure') }}
)

select 
	*
from unique_seq 
where seq > 1