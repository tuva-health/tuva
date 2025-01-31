with ms_drg as (

    select 
          ms_drg_code 
        , ms_drg_description
    from {{ ref('terminology__ms_drg') }}

)

select * 
from ms_drg