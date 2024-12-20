with condition_group_distinct as (

    select distinct 
          condition_group_1
        , condition_group_2
        , condition_group_3
    from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }}

),

condition_group_final as (

    select  
          condition_group_1
        , condition_group_2
        , condition_group_3
        , row_number() over (
            order by 
                  condition_group_1
                , condition_group_2
                , condition_group_3
        ) as condition_group_id
    from condition_group_distinct

)

select * 
from condition_group_final