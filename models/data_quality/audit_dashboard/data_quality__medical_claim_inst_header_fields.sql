{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

select
    'admit_source has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('normalized_input__int_admit_source_voting') }}
where occurrence_row_count > 1

union all

select
    'admit_type has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('normalized_input__int_admit_type_voting') }}
where occurrence_row_count > 1

union all

select
    'apr_drg has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('normalized_input__int_apr_drg_voting') }}
where occurrence_row_count > 1

union all

select
    'bill_type_code has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('normalized_input__int_bill_type_voting') }}
where occurrence_row_count > 1

union all

select
    'discharge_disposition_code has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('normalized_input__int_discharge_disposition_voting') }}
where occurrence_row_count > 1

union all

select
    'ms_drg_code has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('normalized_input__int_ms_drg_voting') }}
where occurrence_row_count > 1

union all

select
    'procedure_code has multiple values' data_quality_check
    , count(distinct claim_id) as result_count
    from {{ ref('normalized_input__int_procedure_code_voting') }}
where occurrence_row_count > 1