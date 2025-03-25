{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select distinct
    product_rxcui as brand_with_generic_available
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy__rxnorm_generic_available') }}
where
    ndc_product_tty in ('SCD', 'GPCK')
    {% if target.type == 'fabric' %}
        and cast(product_startmarketingdate as date) <= GETDATE()
    {% else %}
        and cast(product_startmarketingdate as date) <= current_date
{% endif %}
