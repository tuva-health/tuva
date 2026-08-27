{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
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
