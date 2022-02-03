{{ config(materialized='table', tags='core') }}

select
    patient_id
,   coverage_start_date
,   coverage_end_date
,   primary_payer
,   payer_type
from {{ var('src_coverage') }}