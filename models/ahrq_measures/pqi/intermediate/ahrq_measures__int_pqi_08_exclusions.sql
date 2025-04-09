{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with cardiac as (
    select distinct
        encounter_id
      , data_source
      , 'cardiac procedure' as exclusion_reason
    from {{ ref('ahrq_measures__stg_pqi_procedure') }} as c
    inner join {{ ref('pqi__value_sets') }} as pqi
      on c.normalized_code = pqi.code
      and c.normalized_code_type = 'icd-10-pcs'
      and pqi.value_set_name = 'cardiac_procedure_codes'
      and pqi.pqi_number = 'appendix_b'
    where c.encounter_id is not null
)

, union_cte as (
    select
        encounter_id
      , data_source
      , exclusion_reason
    from {{ ref('ahrq_measures__int_pqi_shared_exclusion_union') }}

    union all

    select
        encounter_id
      , data_source
      , exclusion_reason
    from cardiac
)

select
    encounter_id
  , data_source
  , exclusion_reason
  , row_number() over (
      partition by encounter_id, data_source
      order by exclusion_reason
    ) as exclusion_number
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from union_cte
