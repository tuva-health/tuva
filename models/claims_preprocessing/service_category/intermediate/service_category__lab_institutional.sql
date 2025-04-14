
select distinct
  med.claim_id
, med.claim_line_number
, 'ancillary' as service_category_1
, 'lab' as service_category_2
, 'lab' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
    on med.claim_id = outpatient.claim_id
where substring(med.bill_type_code, 1, 2) in ('14')
or
med.ccs_category in ('233' -- lab
, '235' --other lab
, '234' --pathology
)
