with condition_grouper_distinct as (

    select distinct 
          condition_grouper_1
        , condition_grouper_2
        , condition_grouper_3
    from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }}

),

condition_grouper_final as (

    select  
          condition_grouper_1
        , condition_grouper_2
        , condition_grouper_3
        , row_number() over (
            order by 
                  condition_grouper_1
                , condition_grouper_2
                , condition_grouper_3
        ) as condition_grouper_id
    from condition_grouper_distinct

)

select * 
from condition_grouper_final