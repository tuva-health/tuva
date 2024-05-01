/* Exclude patients with missing age */
select 
    data_source
    , patient_id
from {{ ref('core__patient') }}
where birth_date is null