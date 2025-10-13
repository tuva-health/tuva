{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

-- Pre-aggregated by encounter group and type (no encounter_id grain)
select
  mc.data_source,
  mc.payer,
  mc.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  e.encounter_group,
  e.encounter_type,
  count(distinct mc.encounter_id) as encounters,
  sum(coalesce(mc.paid_amount, 0)) as paid_amount,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__stg_medical_claim') }} mc
join {{ ref('core__encounter') }} e
  on mc.encounter_id = e.encounter_id
 and mc.data_source = e.data_source
group by mc.data_source, mc.payer, mc.{{ quote_column('plan') }}, e.encounter_group, e.encounter_type
