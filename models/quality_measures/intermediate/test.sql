with exclusions as (

select *
from {{ref('shared_exclusions__exclude_hospice_palliative')}}


)

, valid_exclusions as (

  select 
    exclusions.*
  from exclusions
--   inner join {{ref('quality_measures__int_nqf0059_denominator')}} as p
--       on exclusions.patient_id = p.patient_id
--   where exclusions.exclusion_date between p.performance_period_begin and p.performance_period_end
  where exclusions.exclusion_date between '2017-08-09' and '2018-08-08'
)

select * from valid_exclusions