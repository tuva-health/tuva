with hcpcs as (

    select
          hcpcs_cd as hcpcs_code
        , rbcs_family_desc as hcpcs_description
    from {{ ref('terminology__hcpcs_to_rbcs') }}
    where current_flag = 1

)

select * 
from hcpcs