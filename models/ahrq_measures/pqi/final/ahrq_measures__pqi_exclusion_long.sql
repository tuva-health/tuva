{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 1 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_01_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 3 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_03_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 5 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_05_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 7 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_07_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 8 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_08_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 11 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_11_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 12 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_12_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 14 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_14_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 15 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_15_exclusions') }} as e

union all

select
    e.data_source
  , e.encounter_id
  , e.exclusion_reason
  , e.exclusion_number
  , 16 as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_16_exclusions') }} as e
