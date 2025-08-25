{{ config(
    enabled = var('claims_enabled', False)
) }}

with cte as (
    select
        'input_layer' as source
      , 'eligibility' as table_name
      , count(*) as row_count
      , count(distinct person_id) as unique_check
      , 'Unique Patient Count' as unique_check_desc
      , 2 as table_order
    from {{ ref('input_layer__eligibility') }}

    union all

    select
        'raw_data' as source
      , 'eligibility' as table_name
      , null as row_count
      , null as unique_check
      , 'Unique Patient Count' as unique_check_desc
      , 1 as table_order
    from {{ ref('input_layer__eligibility') }}

    union all

    select
        'input_layer' as source
      , 'medical_claim' as table_name
      , count(*) as row_count
      , count(distinct claim_id) as unique_check
      , 'Unique Claim Count' as unique_check_desc
      , 4 as table_order
    from {{ ref('input_layer__medical_claim') }}

    union all

    select
        'raw_data' as source
      , 'medical_claim' as table_name
      , null as row_count
      , null as unique_check
      , 'Unique Claim Count' as unique_check_desc
      , 3 as table_order
    from {{ ref('input_layer__medical_claim') }}

    union all

    select
        'input_layer' as source
      , 'pharmacy_claim' as table_name
      , count(*) as row_count
      , count(distinct claim_id) as unique_check
      , 'Unique Claim Count' as unique_check_desc
      , 6 as table_order
    from {{ ref('input_layer__pharmacy_claim') }}

    union all

    select
        'raw_data' as source
      , 'pharmacy_claim' as table_name
      , null as row_count
      , null as unique_check
      , 'Unique Claim Count' as unique_check_desc
      , 5 as table_order
    from {{ ref('input_layer__pharmacy_claim') }}
)

select
    cte.*
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from cte
