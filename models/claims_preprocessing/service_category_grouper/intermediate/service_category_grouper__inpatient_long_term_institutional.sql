{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    s.claim_id
  , s.data_source
  , 'inpatient' as service_category_1
  , 'inpatient long term acute care' as service_category_2
  , 'inpatient long term acute care' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category_grouper__stg_medical_claim') }} as s
  inner join {{ ref('service_category_grouper__stg_inpatient_institutional') }} as i
  on s.claim_id = i.claim_id
  and s.data_source = i.data_source
where s.primary_taxonomy_code in ('282E00000X')
