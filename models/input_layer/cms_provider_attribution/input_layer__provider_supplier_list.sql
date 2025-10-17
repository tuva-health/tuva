with base as (
select distinct
      performance_year
    , aco_id
    , tin
    , ccn
    , npi
    , specialty
from {{ref('provider_supplier_list')}}
)
select * from base
where npi is not null or ccn is not null