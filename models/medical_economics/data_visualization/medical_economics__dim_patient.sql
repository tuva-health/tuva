with patient as (

    select 
          person_id 
        , first_name 
        , last_name
        , sex
        , race
        , birth_date
        , death_date
        , address
        , city
        , state
        , zip_code
        , county
        , latitude
        , longitude
        , phone 
        , age
        , age_group
        , data_source
    from {{ ref('core__patient') }}

)

select * 
from patient 