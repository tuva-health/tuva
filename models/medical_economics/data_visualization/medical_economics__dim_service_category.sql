with service_category_distinct as (

    select distinct 
          service_category_1
        , service_category_2
        , service_category_3
    from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }}

),

service_category_final as (

    select  
          service_category_1
        , service_category_2
        , service_category_3
        , row_number() over (
            order by 
                  service_category_1
                , service_category_2
                , service_category_3
        ) as service_category_id
    from service_category_distinct

)

select * 
from service_category_final