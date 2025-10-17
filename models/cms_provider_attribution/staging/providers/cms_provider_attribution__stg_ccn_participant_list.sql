select distinct
      performance_year
    , aco_id
    , ccn
    
from {{ref('cms_provider_attribution__stg_provider_supplier_list')}}
where ccn is not null