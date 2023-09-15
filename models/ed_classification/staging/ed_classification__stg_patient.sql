select
    patient_id
    , sex
    , birth_date
    , race
    , state
from {{ ref('core__patient') }}