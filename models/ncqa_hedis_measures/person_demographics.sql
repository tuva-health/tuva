
select
  person_id as person_id,
  sex as sex,
  birth_date as birth_date,
  death_date as death_date,
  race as race,
  ethnicity as ethnicity
from {{ ref('core__patient') }}
