{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select distinct
    med.claim_id
  , med.claim_line_number
  , med.data_source
  , med.claim_line_id
  , 'outpatient' as service_category_1
  , 'home hospice' as service_category_2
  , 'home hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_professional') }} as prof
  on med.claim_id = prof.claim_id
  and med.claim_line_number = prof.claim_line_number
  and med.data_source = prof.data_source
where med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5004')
  and not exists (
    select 1
    from {{ ref('service_category__stg_medical_claim') }} as fac
    where fac.claim_id = med.claim_id
    and fac.data_source = med.data_source
    and fac.claim_type = med.claim_type
    and fac.hcpcs_code in ('Q5005', 'Q5006', 'Q5007', 'Q5008', 'Q5009', 'Q5010')
  )
