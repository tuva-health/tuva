select 
    year as program_year
  , REPLACE(cast(tin as VARCHAR), '''', '') as tin
  , REPLACE(cast(ccn as VARCHAR), '''', '') as ccn
  , REPLACE(cast(npi as VARCHAR), '''', '') as npi
  , specialty
from {{ref('input_layer__practitioner')}}
where npi is not null
