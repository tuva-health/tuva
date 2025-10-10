{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated eligibility terminology metrics at data_source/payer/plan grain.
*/

with gender as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:GENDER' as metric_id,
        'Gender (Eligibility)'      as metric_name,
        'eligibility'               as claim_scope,
        sum(case when term.gender is not null then 1 else 0 end) as valid_n,
        sum(case when e.gender is not null and term.gender is null then 1 else 0 end) as invalid_n,
        sum(case when e.gender is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__gender') }} term on e.gender = term.gender
    group by e.data_source, e.payer, {{ quote_column('plan') }}
),

race as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:RACE' as metric_id,
        'Race (Eligibility)'      as metric_name,
        'eligibility'             as claim_scope,
        sum(case when term.description is not null then 1 else 0 end) as valid_n,
        sum(case when e.race is not null and term.description is null then 1 else 0 end) as invalid_n,
        sum(case when e.race is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__race') }} term on e.race = term.description
    group by e.data_source, e.payer, {{ quote_column('plan') }}
),

payer_type as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:PAYER_TYPE' as metric_id,
        'Payer Type (Eligibility)'      as metric_name,
        'eligibility'                   as claim_scope,
        sum(case when term.payer_type is not null then 1 else 0 end) as valid_n,
        sum(case when e.payer_type is not null and term.payer_type is null then 1 else 0 end) as invalid_n,
        sum(case when e.payer_type is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__payer_type') }} term on e.payer_type = term.payer_type
    group by e.data_source, e.payer, {{ quote_column('plan') }}
),

medicare_status as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:MEDICARE_STATUS_CODE' as metric_id,
        'Medicare Status Code (Eligibility)'      as metric_name,
        'eligibility'                              as claim_scope,
        sum(case when term.medicare_status_code is not null then 1 else 0 end) as valid_n,
        sum(case when e.medicare_status_code is not null and term.medicare_status_code is null then 1 else 0 end) as invalid_n,
        sum(case when e.medicare_status_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_status') }} term on e.medicare_status_code = term.medicare_status_code
    group by e.data_source, e.payer, {{ quote_column('plan') }}
),

dual_status as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:DUAL_STATUS_CODE' as metric_id,
        'Dual Status Code (Eligibility)'      as metric_name,
        'eligibility'                          as claim_scope,
        sum(case when term.dual_status_code is not null then 1 else 0 end) as valid_n,
        sum(case when e.dual_status_code is not null and term.dual_status_code is null then 1 else 0 end) as invalid_n,
        sum(case when e.dual_status_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_dual_eligibility') }} term on e.dual_status_code = term.dual_status_code
    group by e.data_source, e.payer, {{ quote_column('plan') }}
),

orec as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:ORIGINAL_REASON_ENTITLEMENT_CODE' as metric_id,
        'Original Reason Entitlement Code (Eligibility)'       as metric_name,
        'eligibility'                                          as claim_scope,
        sum(case when term.original_reason_entitlement_code is not null then 1 else 0 end) as valid_n,
        sum(case when e.original_reason_entitlement_code is not null and term.original_reason_entitlement_code is null then 1 else 0 end) as invalid_n,
        sum(case when e.original_reason_entitlement_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_orec') }} term on e.original_reason_entitlement_code = term.original_reason_entitlement_code
    group by e.data_source, e.payer, {{ quote_column('plan') }}
)

select * from gender
union all select * from race
union all select * from payer_type
union all select * from medicare_status
union all select * from dual_status
union all select * from orec

