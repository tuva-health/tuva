
{{ config(materialized='view') }}

select
    patient_id
,   coverage_start_date
,   coverage_end_date
,   primary_payer
,   payer_type
from {{ var('stg_coverage') }}