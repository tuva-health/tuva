select 
    year as program_year
  , REPLACE(cast(tin as VARCHAR), '''', '') as tin
  , REPLACE(cast(ccn as VARCHAR), '''', '') as ccn
  , REPLACE(cast(npi as VARCHAR), '''', '') as npi
  , specialty
from {{ref('input_layer__provider_supplier_list')}}
where npi is not null or ccn is not null
