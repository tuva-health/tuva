with random_group_union as (

    select distinct person_id
    from {{ ref('medical_economics__member_months_random_ten_percent') }}
union all 
    select distinct person_id
    from {{ ref('medical_economics__member_months_random_twenty_percent') }}

),

random_group_final as (

    select distinct person_id 
    from random_group_union 

)

select aa.*
from {{ ref('core__patient') }} aa 
inner join random_group_final bb    
    on aa.person_id = bb.person_id