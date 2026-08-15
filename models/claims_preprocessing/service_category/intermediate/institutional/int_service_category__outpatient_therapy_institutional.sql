{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct
    med.claim_id
    , med.claim_line_number
    , med.data_source
    , 'outpatient' as service_category_1
    , 'outpatient pt/ot/st' as service_category_2
    , 'outpatient pt/ot/st' as service_category_3
    , '{{ this.name }}' as source_model_name
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('stg_service_category__medical_claim') }} as med
inner join {{ ref('int_service_category__outpatient_institutional') }} as o
  on med.claim_id = o.claim_id
  and med.data_source = o.data_source
where ccs_category in ('213', '212', '215')
