with unique_seq as (
select
	*,
	row_number() over(partition by patient_id, coverage_start_date, coverage_end_date, primary_payer, payer_type order by 1) as seq
from {{ ref('stg_coverage') }}
)

select 
	*
from unique_seq 
where seq > 1