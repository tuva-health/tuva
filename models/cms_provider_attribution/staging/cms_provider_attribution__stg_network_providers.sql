-- This file includes all providers in the network which cannot be assigned to a different ACO
select
    tin
  , ccn
  , npi
from {{ref('input_layer__network_providers')}}
    