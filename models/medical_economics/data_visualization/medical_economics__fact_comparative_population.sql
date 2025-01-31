with population_1 as (

    select 
          person_id 
        , 1 as comparative_population_id
        , 'California' as comparative_population
    from {{ ref('medical_economics__dim_patient') }}
    where state = 'California'

),

population_2 as (

    select 
          person_id 
        , 2 as comparative_population_id
        , 'All other states' as comparative_population
    from {{ ref('medical_economics__dim_patient') }}
    where state <> 'California'

),

combine_union as (

    select *
    from population_1
union all 
    select * 
    from population_2

)

select 
      person_id
    , comparative_population_id
    , comparative_population
from combine_union