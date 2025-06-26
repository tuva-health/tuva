select
    person_id
    , sex
    , birth_date
    , state
    , race
from {{ ref('core__patient') }}
