{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

select
  mc.data_source,
  mc.payer,
  mc.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  e.encounter_group,
  count(distinct mc.encounter_id) as encounters,
  sum(coalesce(mc.paid_amount,0)) as paid_amount,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__stg_medical_claim') }} mc
left join {{ ref('core__encounter') }} e
  on mc.encounter_id = e.encounter_id
group by mc.data_source, mc.payer, mc.{{ quote_column('plan') }}, e.encounter_group

