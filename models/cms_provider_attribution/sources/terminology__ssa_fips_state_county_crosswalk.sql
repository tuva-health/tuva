-- NOTE: This is temporary until this can be integrated into the Tuva project
select
    *

from {{source('terminology', 'ssa_fips_state_county_crosswalk')}}