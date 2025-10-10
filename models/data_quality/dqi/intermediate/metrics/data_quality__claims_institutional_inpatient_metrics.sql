{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated institutional inpatient metrics (DRG, ICD-10-PCS procedures 1-3)
at data_source/payer/plan grain.
*/

with drg as (
    with base as (
        select * from {{ ref('medical_claim') }}
        where claim_type = 'institutional'
          and {{ substring('bill_type_code', 1, 2) }} = '11'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.drg_code is not null then b.drg_code end) as distinct_vals,
            max(case when b.drg_code is not null and (
                (b.drg_code_type='ms-drg' and ms.ms_drg_code is not null) or
                (b.drg_code_type='apr-drg' and apr.apr_drg_code is not null)
            ) then 1 else 0 end) as has_valid,
            max(case when b.drg_code is not null and (
                (b.drg_code_type='ms-drg' and ms.ms_drg_code is null) or
                (b.drg_code_type='apr-drg' and apr.apr_drg_code is null)
            ) then 1 else 0 end) as has_invalid,
            max(case when b.drg_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__ms_drg') }} ms on b.drg_code_type='ms-drg' and b.drg_code = ms.ms_drg_code
        left join {{ ref('terminology__apr_drg') }} apr on b.drg_code_type='apr-drg' and b.drg_code = apr.apr_drg_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:DRG_CODE' as metric_id,
        'DRG Code (Inpatient)'                    as metric_name,
        'institutional_inpatient'                 as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

proc1 as (
    with base as (
        select * from {{ ref('medical_claim') }}
        where claim_type = 'institutional' and {{ substring('bill_type_code', 1, 2) }} = '11'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.procedure_code_1 is not null then b.procedure_code_1 end) as distinct_vals,
            max(case when b.procedure_code_1 is not null and term.icd_10_pcs is not null then 1 else 0 end) as has_valid,
            max(case when b.procedure_code_1 is not null and term.icd_10_pcs is null then 1 else 0 end) as has_invalid,
            max(case when b.procedure_code_1 is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_1 = term.icd_10_pcs
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:PROCEDURE_CODE_1' as metric_id,
        'Procedure Code 1 (Inpatient)'                    as metric_name,
        'institutional_inpatient'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

proc2 as (
    with base as (
        select * from {{ ref('medical_claim') }}
        where claim_type = 'institutional' and {{ substring('bill_type_code', 1, 2) }} = '11'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.procedure_code_2 is not null then b.procedure_code_2 end) as distinct_vals,
            max(case when b.procedure_code_2 is not null and term.icd_10_pcs is not null then 1 else 0 end) as has_valid,
            max(case when b.procedure_code_2 is not null and term.icd_10_pcs is null then 1 else 0 end) as has_invalid,
            max(case when b.procedure_code_2 is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_2 = term.icd_10_pcs
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:PROCEDURE_CODE_2' as metric_id,
        'Procedure Code 2 (Inpatient)'                    as metric_name,
        'institutional_inpatient'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

proc3 as (
    with base as (
        select * from {{ ref('medical_claim') }}
        where claim_type = 'institutional' and {{ substring('bill_type_code', 1, 2) }} = '11'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.procedure_code_3 is not null then b.procedure_code_3 end) as distinct_vals,
            max(case when b.procedure_code_3 is not null and term.icd_10_pcs is not null then 1 else 0 end) as has_valid,
            max(case when b.procedure_code_3 is not null and term.icd_10_pcs is null then 1 else 0 end) as has_invalid,
            max(case when b.procedure_code_3 is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_3 = term.icd_10_pcs
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:PROCEDURE_CODE_3' as metric_id,
        'Procedure Code 3 (Inpatient)'                    as metric_name,
        'institutional_inpatient'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
)

select * from drg
union all select * from proc1
union all select * from proc2
union all select * from proc3

