{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 1 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_01_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 3 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_03_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 5 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_05_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 7 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_07_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 8 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_08_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 11 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_11_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 12 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_12_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 14 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_14_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 15 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_15_denom') }} as d

union all

select
    d.year_number
  , d.patient_id
  , d.data_source
  , 16 as pqi_number
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('ahrq_measures__int_pqi_16_denom') }} as d
