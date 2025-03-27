{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '01' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_01_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '03' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_03_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '05' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_05_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '07' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_07_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '08' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_08_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '11' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_11_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '12' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_12_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '14' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_14_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '15' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_15_num') }} as n

union all

select
    n.data_source
  , n.person_id
  , n.year_number
  , n.encounter_id
  , '16' as pqi_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_16_num') }} as n
