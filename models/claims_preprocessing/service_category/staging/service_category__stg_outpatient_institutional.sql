{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select distinct
    a.claim_id
  , a.data_source
  , 'outpatient' as service_type
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
left outer join {{ ref('service_category__stg_inpatient_institutional') }} as i
  on a.claim_id = i.claim_id
  and a.data_source = i.data_source
where i.claim_id is null
  and a.claim_type = 'institutional'
