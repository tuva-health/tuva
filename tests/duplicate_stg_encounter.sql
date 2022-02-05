with unique_seq as (
select
	*,
	row_number() over(partition by encounter_id, patient_id, encounter_start_date, encounter_end_date, encounter_type,
        admit_type_code, admit_source_code, discharge_status_code, attending_provider_npi, facility_npi, drg, paid_amount order by 1) as seq
from {{ ref('stg_encounter') }}
)

select 
	*
from unique_seq 
where seq > 1