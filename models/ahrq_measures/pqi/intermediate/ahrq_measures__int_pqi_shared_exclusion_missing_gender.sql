-- Exclude patients with undefined or missing gender
select 
    data_source
  , patient_id
from {{ ref('core__patient') }}
where 
  sex not in ('male', 'female')
