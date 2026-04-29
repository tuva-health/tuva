{{ config(
    enabled = (var('enable_legacy_data_quality', false) | as_bool)
     and (var('claims_enabled', False) | as_bool)
) }}

select *
from {{ ref('pharmacy__generic_available_list') }}
