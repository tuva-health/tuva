{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

with encounter_payer_plan as (
  select distinct data_source, payer, {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
)

select
  i.data_source,
  epp.payer,
  epp.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  i.drg_code,
  i.drgwithdescription,
  count(distinct i.encounter_id) as encounters,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__inpatient') }} i
left join encounter_payer_plan epp
  on i.data_source = epp.data_source and i.encounter_id = epp.encounter_id
where i.drg_code is not null
group by i.data_source, epp.payer, epp.{{ quote_column('plan') }}, i.drg_code, i.drgwithdescription

