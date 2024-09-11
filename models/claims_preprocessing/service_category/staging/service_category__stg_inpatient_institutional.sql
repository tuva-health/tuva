{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with drg_requirement as (
  select distinct
      mc.claim_id
  from {{ ref('service_category__stg_medical_claim') }} as mc
  left join {{ ref('terminology__ms_drg') }} as msdrg
    on mc.ms_drg_code = msdrg.ms_drg_code
  left join {{ ref('terminology__apr_drg') }} as aprdrg
    on mc.apr_drg_code = aprdrg.apr_drg_code
  where mc.claim_type = 'institutional'
    and (
      msdrg.ms_drg_code is not null
      or aprdrg.apr_drg_code is not null
    )
)

, bill_type_requirement as (
  select distinct
      claim_id
  from {{ ref('service_category__stg_medical_claim') }}
  where claim_type = 'institutional'
    and substring(bill_type_code, 1, 2) in (
      '11'  -- hospital inpatient 
    , '12'  -- hospital inpatient 
    , '21'  -- SNF inpatient
    , '82'  -- inpatient hospice
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
