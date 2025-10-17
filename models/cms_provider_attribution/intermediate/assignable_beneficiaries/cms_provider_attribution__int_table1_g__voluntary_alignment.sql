select 
      alr.aco_id
    , alr.performance_year
    , alr.person_id
    , alr.va_npi as npi
    , alr.va_tin as tin
    , 0 as step
from {{ref('cms_provider_attribution__stg_aalr1')}} alr
inner join {{ref('cms_provider_attribution__stg_provider_supplier_list')}}  prov_supp
    on alr.va_npi = prov_supp.npi
    and alr.va_tin = prov_supp.tin
where in_va_max = 1