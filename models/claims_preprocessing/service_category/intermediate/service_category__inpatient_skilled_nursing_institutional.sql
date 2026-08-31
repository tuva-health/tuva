{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select distinct
  claim_id
  , data_source
  , 'inpatient' as service_category_1
  , 'skilled nursing' as service_category_2
  , 'inpatient part A' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and substring(bill_type_code, 1, 2) in ('21')

{{ the_tuva_project.union_distinct() }}

select distinct
  claim_id
  , data_source
  , 'inpatient' as service_category_1
  , 'skilled nursing' as service_category_2
  , 'inpatient part B' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and substring(bill_type_code, 1, 2) in ('22')
