{{ config(materialized='table') }}

select
    cast(physician_npi as varchar) as physician_npi
,   cast(name as varchar) as name
,   cast(specialty as varchar) as specialty
,   cast(sub_specialty as varchar) as sub_specialty
,   cast(data_source as varchar) as data_source
from {{ var('src_practitioner') }}