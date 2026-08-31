{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

select
    hcpcs_cd
  , first_rbcs_release_year
  , count(*) as row_count
from {{ ref('terminology__restructured_betos') }}
group by
    hcpcs_cd
  , first_rbcs_release_year
having
    hcpcs_cd is null
    or first_rbcs_release_year is null
    or count(*) > 1
