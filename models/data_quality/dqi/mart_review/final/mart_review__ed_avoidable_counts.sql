{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

with encounter_payer_plan as (
  select distinct data_source, payer, {{ quote_column('plan') }}, encounter_id
  from {{ ref('mart_review__stg_medical_claim') }}
)

select
  ed.data_source,
  epp.payer,
  epp.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  ed.avoidable_category,
  count(distinct ed.encounter_id) as encounters,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__ed_classification') }} ed
left join encounter_payer_plan epp
  on ed.data_source = epp.data_source and ed.encounter_id = epp.encounter_id
group by ed.data_source, epp.payer, epp.{{ quote_column('plan') }}, ed.avoidable_category

