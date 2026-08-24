{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
    eligibility_id
from {{ ref('normalized__eligibility') }}
where eligibility_id <> {{ concat_custom([
    "person_id",
    "'-'",
    "member_id",
    "'-'",
    "enrollment_start_date",
    "'-'",
    "payer",
    "'-'",
    quote_column('plan'),
    "'-'",
    "data_source"
]) }}
