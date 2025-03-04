with random_1 as (

    select distinct person_id
    from {{ ref('medical_economics__medical_claim_random_ten_percent') }}

),

random_2 as (

    select distinct person_id
    from {{ ref('medical_economics__medical_claim_random_twenty_percent') }}

),

population_1 as (

    select 
          aa.person_id 
        , 1 as comparative_population_id
        , 'Random 10%' as comparative_population
    from {{ ref('medical_economics__dim_patient') }} aa
    inner join random_1 bb 
        on aa.person_id = bb.person_id

),

population_2 as (

    select 
          aa.person_id 
        , 2 as comparative_population_id
        , 'Random 10%' as comparative_population
    from {{ ref('medical_economics__dim_patient') }} aa
    inner join random_2 bb 
        on aa.person_id = bb.person_id

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