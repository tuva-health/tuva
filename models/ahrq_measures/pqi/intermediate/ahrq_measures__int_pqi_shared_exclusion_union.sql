{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

-- Missing Age Exclusion
select
    denom.encounter_id
  , denom.data_source
  , 'missing age' as exclusion_reason
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as denom
inner join {{ ref('ahrq_measures__int_pqi_shared_exclusion_missing_age') }} as age
  on denom.person_id = age.person_id
  and denom.data_source = age.data_source

union all

-- Missing Gender Exclusion
select
    denom.encounter_id
  , denom.data_source
  , 'missing gender' as exclusion_reason
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as denom
inner join {{ ref('ahrq_measures__int_pqi_shared_exclusion_missing_gender') }} as gender
  on denom.person_id = gender.person_id
  and denom.data_source = gender.data_source

union all

-- Missing Dates Exclusion
select
    denom.encounter_id
  , denom.data_source
  , 'missing dates' as exclusion_reason
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as denom
inner join {{ ref('ahrq_measures__int_pqi_shared_exclusion_missing_dates') }} as dates
  on denom.encounter_id = dates.encounter_id
  and denom.data_source = dates.data_source

union all

-- Missing Primary Diagnosis Exclusion
select
    denom.encounter_id
  , denom.data_source
  , 'missing primary diagnosis' as exclusion_reason
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as denom
inner join {{ ref('ahrq_measures__int_pqi_shared_exclusion_missing_primary_dx') }} as dx
  on denom.encounter_id = dx.encounter_id
  and denom.data_source = dx.data_source

union all

-- Transfer Exclusion
select
    denom.encounter_id
  , denom.data_source
  , 'transfer' as exclusion_reason
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as denom
inner join {{ ref('ahrq_measures__int_pqi_shared_exclusion_transfer') }} as tx
  on denom.encounter_id = tx.encounter_id
  and denom.data_source = tx.data_source

union all

-- Ungroupable DRG Exclusion
select
    denom.encounter_id
  , denom.data_source
  , 'ungroupable DRG' as exclusion_reason
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }} as denom
inner join {{ ref('ahrq_measures__int_pqi_shared_exclusion_ungroupable_drg') }} as drg
  on denom.encounter_id = drg.encounter_id
  and denom.data_source = drg.data_source
