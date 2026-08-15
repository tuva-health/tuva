{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct
    med.claim_id
  , med.claim_line_number
  , med.data_source
  , 'ancillary' as service_category_1
  , 'lab' as service_category_2
  , 'lab' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('stg_service_category__medical_claim') }} as med
inner join {{ ref('int_service_category__outpatient_institutional') }} as outpatient
    on med.claim_id = outpatient.claim_id
    and med.data_source = outpatient.data_source
where substring(med.bill_type_code, 1, 2) in ('14')
or med.ccs_category in (
    '233' -- lab
  , '235' --other lab
  , '234' --pathology
)
