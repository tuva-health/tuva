select distinct 
    product_rxcui as brand_with_generic_available
from {{ ref('pharmacy__rxnorm_generic_available') }}
where 
    ndc_product_tty in ('SCD', 'GPCK')
  and cast(product_startmarketingdate as date) <= current_date
