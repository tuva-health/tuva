{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/* Centralized eligibility terminology checks (record-level). */

with e as (
    select * from {{ ref('eligibility') }}
)

select
    e.data_source,
    e.payer,
    {{ quote_column('plan') }} as plan,
    cast(null as {{ dbt.type_string() }}) as claim_id,
    cast(null as {{ dbt.type_int() }}) as claim_line_number,
    metric_id,
    'eligibility' as claim_scope,
    1 as distinct_vals,
    has_valid,
    has_invalid,
    has_null
from (
    -- Gender
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        null as claim_id,
        null as claim_line_number,
        'claims:eligibility:GENDER' as metric_id,
        case when term.gender is not null then 1 else 0 end as has_valid,
        case when e.gender is not null and term.gender is null then 1 else 0 end as has_invalid,
        case when e.gender is null then 1 else 0 end as has_null
    from e
    left join {{ ref('terminology__gender') }} term on e.gender = term.gender

    union all
    -- Race
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        null,
        null,
        'claims:eligibility:RACE' as metric_id,
        case when term.description is not null then 1 else 0 end as has_valid,
        case when e.race is not null and term.description is null then 1 else 0 end as has_invalid,
        case when e.race is null then 1 else 0 end as has_null
    from e
    left join {{ ref('terminology__race') }} term on e.race = term.description

    union all
    -- Payer Type
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        null,
        null,
        'claims:eligibility:PAYER_TYPE' as metric_id,
        case when term.payer_type is not null then 1 else 0 end as has_valid,
        case when e.payer_type is not null and term.payer_type is null then 1 else 0 end as has_invalid,
        case when e.payer_type is null then 1 else 0 end as has_null
    from e
    left join {{ ref('terminology__payer_type') }} term on e.payer_type = term.payer_type

    union all
    -- Medicare Status
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        null,
        null,
        'claims:eligibility:MEDICARE_STATUS_CODE' as metric_id,
        case when term.medicare_status_code is not null then 1 else 0 end as has_valid,
        case when e.medicare_status_code is not null and term.medicare_status_code is null then 1 else 0 end as has_invalid,
        case when e.medicare_status_code is null then 1 else 0 end as has_null
    from e
    left join {{ ref('terminology__medicare_status') }} term on e.medicare_status_code = term.medicare_status_code

    union all
    -- Dual Status
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        null,
        null,
        'claims:eligibility:DUAL_STATUS_CODE' as metric_id,
        case when term.dual_status_code is not null then 1 else 0 end as has_valid,
        case when e.dual_status_code is not null and term.dual_status_code is null then 1 else 0 end as has_invalid,
        case when e.dual_status_code is null then 1 else 0 end as has_null
    from e
    left join {{ ref('terminology__medicare_dual_eligibility') }} term on e.dual_status_code = term.dual_status_code

    union all
    -- Original Reason Entitlement Code (OREC)
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        null,
        null,
        'claims:eligibility:ORIGINAL_REASON_ENTITLEMENT_CODE' as metric_id,
        case when term.original_reason_entitlement_code is not null then 1 else 0 end as has_valid,
        case when e.original_reason_entitlement_code is not null and term.original_reason_entitlement_code is null then 1 else 0 end as has_invalid,
        case when e.original_reason_entitlement_code is null then 1 else 0 end as has_null
    from e
    left join {{ ref('terminology__medicare_orec') }} term on e.original_reason_entitlement_code = term.original_reason_entitlement_code
) t

