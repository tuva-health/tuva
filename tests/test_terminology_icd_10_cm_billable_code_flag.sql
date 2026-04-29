{{ config(
     tags = ['terminology'],
     severity = 'error'
   )
}}

with invalid_flags as (
    select
        icd_10_cm
        , billable_code_flag
        , 'billable_code_flag must be 0 or 1' as failure_reason
    from {{ ref('terminology__icd_10_cm') }}
    where billable_code_flag is null
       or billable_code_flag not in ('0', '1')
)

, expected_values as (
    select 'A00' as icd_10_cm, '0' as expected_billable_code_flag
    union all
    select 'A000' as icd_10_cm, '1' as expected_billable_code_flag
    union all
    select 'A010' as icd_10_cm, '0' as expected_billable_code_flag
    union all
    select 'A0100' as icd_10_cm, '1' as expected_billable_code_flag
)

, anchor_failures as (
    select
        expected_values.icd_10_cm
        , terminology__icd_10_cm.billable_code_flag
        , 'billable_code_flag does not match expected CMS anchor value' as failure_reason
    from expected_values
    left join {{ ref('terminology__icd_10_cm') }} as terminology__icd_10_cm
        on expected_values.icd_10_cm = terminology__icd_10_cm.icd_10_cm
    where terminology__icd_10_cm.icd_10_cm is null
       or terminology__icd_10_cm.billable_code_flag <> expected_values.expected_billable_code_flag
)

select
    icd_10_cm
    , billable_code_flag
    , failure_reason
from invalid_flags

union all

select
    icd_10_cm
    , billable_code_flag
    , failure_reason
from anchor_failures
