with clean as (
select distinct
      cast("program_year" as INT) as year
    , cast("acoid" as VARCHAR(8000)) as aco_id
    , REPLACE(cast("participanttin" as VARCHAR(8000)), '''', '') as tin
    , REPLACE(cast("ccn" as VARCHAR(8000)), '''', '') as ccn
    , REPLACE(cast("individualnpi" as VARCHAR(8000)), '''', '') as npi
    , REPLACE(cast("individualnpi" as VARCHAR(8000)), '''', '') as practitioner_id
    , null as first_name
    , null as last_name
    , null as data_source
    , null as practice_affiliation
    , null as sub_specialty
    , cast("specialty" as VARCHAR(8000)) as specialty
from {{source('phds_lakehouse_test', 'provider_attr_provider_and_supplier_list_a5321')}}
)
select * from clean
where 1=1
    and npi is not null
