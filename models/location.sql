{{ config(materialized='table') }}

select
    cast(facility_npi as varchar) as facility_npi
,   cast(facility_name as varchar) as facility_name
,   cast(facility_type as varchar) as facility_type
,   cast(hospital_type as varchar) as hospital_type
,   cast(parent_organization as varchar) as parent_organization
,   cast(data_source as varchar) as data_source
from {{ var('src_location') }}