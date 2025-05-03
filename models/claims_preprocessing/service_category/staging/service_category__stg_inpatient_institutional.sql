{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with drg_requirement as (
  select distinct
      mc.claim_id
  from {{ ref('service_category__stg_medical_claim') }} as mc
  left outer join {{ ref('terminology__ms_drg') }} as msdrg
    on mc.drg_code_type = 'ms-drg'
    and mc.drg_code = msdrg.ms_drg_code
  left outer join {{ ref('terminology__apr_drg') }} as aprdrg
    on mc.drg_code_type = 'apr-drg'
    and mc.drg_code = aprdrg.apr_drg_code
  where mc.claim_type = 'institutional'
    and (msdrg.ms_drg_code is not null
      or aprdrg.apr_drg_code is not null
    )
)

, bill_type_requirement as (
  select distinct
      claim_id
  from {{ ref('service_category__stg_medical_claim') }}
  where claim_type = 'institutional'
    and substring(bill_type_code, 1, 2) in (
      '11'  -- Hospital Inpatient (Part A)
    , '12'  -- Hospital Inpatient (Part B)
    , '21'  -- Skilled Nursing Facility (SNF) Inpatient (Part A)
    , '82'  -- Hospital-based Hospice (Inpatient)
    , '15'  -- Hospital Intermediate Care - Level I
    , '16'  -- Hospital Intermediate Care - Level II
    , '17'  -- Hospital Subacute Inpatient
    , '18'  -- Hospital Swing Beds
    , '22'  -- Skilled Nursing Facility (SNF) Inpatient (Part B)
    , '25'  -- SNF Intermediate Care - Level I
    , '26'  -- SNF Intermediate Care - Level II
    , '27'  -- SNF Subacute Inpatient
    , '28'  -- SNF Swing Beds
    , '31'  -- Home Health Inpatient (Part A)
    , '41'  -- Religious Nonmedical Hospital Inpatient (Part A)
    , '42'  -- Religious Nonmedical Hospital Inpatient (Part B)
    , '45'  -- Religious Nonmedical Hospital Intermediate Care - Level I
    , '46'  -- Religious Nonmedical Hospital Intermediate Care - Level II
    , '47'  -- Religious Nonmedical Hospital Subacute Inpatient
    , '48'  -- Religious Nonmedical Hospital Swing Beds
    , '61'  -- Intermediate Care Inpatient (Part A)
    , '62'  -- Intermediate Care Inpatient (Part B)
    , '65'  -- Intermediate Care - Level I
    , '66'  -- Intermediate Care - Level II
    , '67'  -- Intermediate Care Subacute Inpatient
    , '68'  -- Intermediate Care Swing Beds
    )
)

select distinct
    a.claim_id
  , 'inpatient' as service_type
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
inner join bill_type_requirement as d
  on a.claim_id = d.claim_id

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct
    a.claim_id
  , 'inpatient' as service_type
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
inner join drg_requirement as c
  on a.claim_id = c.claim_id
