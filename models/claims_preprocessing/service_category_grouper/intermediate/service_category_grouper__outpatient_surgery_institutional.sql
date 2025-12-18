{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    med.claim_id --claim level
  , med.data_source
  , 'outpatient' as service_category_1
  , 'outpatient surgery' as service_category_2
  , 'outpatient surgery' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category_grouper__stg_medical_claim') }} as med
inner join {{ ref('service_category_grouper__stg_outpatient_institutional') }} as o
  on med.claim_id = o.claim_id
  and med.data_source = o.data_source
where ccs_category between '1' and '176'
  or ccs_category in ('229', '230', '231', '232', '244')
