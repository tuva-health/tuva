with apr_drg as (

    select 
          apr_drg_code 
        , apr_drg_description
    from {{ ref('terminology__apr_drg') }}

)

select * 
from apr_drg