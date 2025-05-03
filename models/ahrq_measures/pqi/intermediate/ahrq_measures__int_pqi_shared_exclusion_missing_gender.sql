{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

-- Exclude patients with undefined or missing gender
select
    data_source
  , person_id
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_patient') }}
where
  sex not in ('male', 'female')
