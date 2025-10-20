-- This file includes all providers in the network which cannot be assigned to a different ACO
-- This is an optional file, if not wanted, pass in nulls for all values
select 
      tin
    , ccn
    , npi
from {{ref('network_providers')}}