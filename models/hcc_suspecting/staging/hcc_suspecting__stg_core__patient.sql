select
      person_id
    , sex
    , birth_date
    , death_date
from {{ ref('core__patient') }}
