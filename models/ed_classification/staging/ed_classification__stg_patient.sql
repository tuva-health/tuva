
select
    person_id
    , sex
    , birth_date
    , race
    , state
    , zip_code
    , latitude
    , longitude
from {{ ref('core__patient') }}
