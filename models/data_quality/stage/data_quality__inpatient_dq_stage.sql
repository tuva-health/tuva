{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False)))) | as_bool
)}}

with drg_requirement as (
  select distinct
      mc.claim_id
  from {{ ref('input_layer__medical_claim') }} as mc
  left join {{ ref('terminology__ms_drg') }} as msdrg
    on mc.drg_code = msdrg.ms_drg_code
  where mc.claim_type = 'institutional'
    and msdrg.ms_drg_code is not null
)

, bill_type_requirement as (
  select distinct
      claim_id
  from {{ ref('input_layer__medical_claim') }}
  where claim_type = 'institutional'
    and substring(bill_type_code, 1, 2) in (
      '11'  -- hospital inpatient 
    , '12'  -- hospital inpatient 
    )
)

select distinct
    a.claim_id
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('input_layer__medical_claim') }} as a
inner join bill_type_requirement as d
  on a.claim_id = d.claim_id

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct
    a.claim_id
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('input_layer__medical_claim') }} as a
inner join drg_requirement as c
  on a.claim_id = c.claim_id
