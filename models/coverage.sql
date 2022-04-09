{{ config(materialized='table') }}

select
    cast(patient_id as varchar) as patient_id
,   cast(coverage_start_date as date) as coverage_start_date
,   cast(coverage_end_date as date) as coverage_end_date
,   cast(payer as varchar) as payer
,   cast(payer_type as varchar) as payer_type
,   cast(data_source as varchar) as data_source
from {{ var('src_coverage') }}