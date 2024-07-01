{% if var('claims_enabled', False) == true  -%}

select
    data_source
    ,claim_type
    ,count(distinct claim_id) as claim_count
    ,sum(paid_amount) as paid_amount
from {{ ref('medical_claim')}}
group by
    data_source
    ,claim_type

{% else -%}

select
    null as data_source,
    null as claim_type,
    null as claim_count,
    null as paid_amount
from VALUES(1)

{%- endif %}