{{ config(
    enabled = var('claims_enabled', False) | as_bool
) }}

select *
from {{ ref('pharmacy__generic_available_list') }}
