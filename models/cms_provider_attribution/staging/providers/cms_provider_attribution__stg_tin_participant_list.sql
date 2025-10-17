select distinct
      performance_year
    , aco_id
    , tin
from {{ref('cms_provider_attribution__stg_provider_supplier_list')}}
where tin is not null