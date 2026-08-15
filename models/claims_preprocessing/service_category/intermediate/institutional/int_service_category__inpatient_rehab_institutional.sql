{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct
    s.claim_id
  , s.data_source
  , 'inpatient' as service_category_1
  , 'inpatient rehabilitation' as service_category_2
  , 'inpatient rehabilitation' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('stg_service_category__medical_claim') }} as s
  inner join {{ ref('int_service_category__inpatient_institutional') }} as i
  on s.claim_id = i.claim_id
  and s.data_source = i.data_source
where s.primary_taxonomy_code in ('283X00000X'
                                  , '273Y00000X'
                                  )
