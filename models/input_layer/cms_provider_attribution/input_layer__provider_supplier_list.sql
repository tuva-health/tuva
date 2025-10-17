with base as (
select distinct
      cast("program_year" as INT) as performance_year
    , cast("acoid" as VARCHAR(8000)) as aco_id
    , REPLACE(cast("participanttin" as VARCHAR(8000)), '''', '') as tin
    , REPLACE(cast("ccn" as VARCHAR(8000)), '''', '') as ccn
    , REPLACE(cast("individualnpi" as VARCHAR(8000)), '''', '') as npi
    , cast("specialty" as VARCHAR(8000)) as specialty
from {{ref('provider_supplier_list')}}
)
select * from base
where npi is not null or ccn is not null