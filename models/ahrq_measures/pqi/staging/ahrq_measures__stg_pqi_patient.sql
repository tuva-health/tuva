
select
    data_source
  , birth_date
  , person_id
  , sex
from
    {{ ref('core__patient') }}
