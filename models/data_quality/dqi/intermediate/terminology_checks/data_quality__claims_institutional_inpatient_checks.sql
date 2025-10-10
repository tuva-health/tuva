{{ config(
    materialized='ephemeral',
    tags=['dqi','data_quality'],
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Per-claim terminology checks for institutional inpatient claims.
Outputs one row per claim per metric with flags used by aggregators:
 - data_source, payer, plan, claim_id, metric_id, claim_scope,
   distinct_vals, has_valid, has_invalid, has_null

This centralizes DRG and ICD-10-PCS (procedure_code_1-3) checks so
downstream metrics can aggregate without re-implementing validations.
*/

with base as (
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        drg_code,
        drg_code_type,
        procedure_code_1,
        procedure_code_2,
        procedure_code_3
    from {{ ref('medical_claim') }}
    where claim_type = 'institutional'
      and {{ substring('bill_type_code', 1, 2) }} = '11'
),

drg_per_claim as (
    select
        b.data_source,
        b.payer,
        {{ quote_column('plan') }} as plan,
        b.claim_id,
        'claims:institutional_inpatient:DRG_CODE' as metric_id,
        'institutional_inpatient' as claim_scope,
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
),

proc1_per_claim as (
    select
        b.data_source,
        b.payer,
        {{ quote_column('plan') }} as plan,
        b.claim_id,
        'claims:institutional_inpatient:PROCEDURE_CODE_1' as metric_id,
        'institutional_inpatient' as claim_scope,
        count(distinct case when b.procedure_code_1 is not null then b.procedure_code_1 end) as distinct_vals,
        max(case when b.procedure_code_1 is not null and term.icd_10_pcs is not null then 1 else 0 end) as has_valid,
        max(case when b.procedure_code_1 is not null and term.icd_10_pcs is null then 1 else 0 end) as has_invalid,
        max(case when b.procedure_code_1 is null then 1 else 0 end) as has_null
    from base b
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_1 = term.icd_10_pcs
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

proc2_per_claim as (
    select
        b.data_source,
        b.payer,
        {{ quote_column('plan') }} as plan,
        b.claim_id,
        'claims:institutional_inpatient:PROCEDURE_CODE_2' as metric_id,
        'institutional_inpatient' as claim_scope,
        count(distinct case when b.procedure_code_2 is not null then b.procedure_code_2 end) as distinct_vals,
        max(case when b.procedure_code_2 is not null and term.icd_10_pcs is not null then 1 else 0 end) as has_valid,
        max(case when b.procedure_code_2 is not null and term.icd_10_pcs is null then 1 else 0 end) as has_invalid,
        max(case when b.procedure_code_2 is null then 1 else 0 end) as has_null
    from base b
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_2 = term.icd_10_pcs
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

proc3_per_claim as (
    select
        b.data_source,
        b.payer,
        {{ quote_column('plan') }} as plan,
        b.claim_id,
        'claims:institutional_inpatient:PROCEDURE_CODE_3' as metric_id,
        'institutional_inpatient' as claim_scope,
        count(distinct case when b.procedure_code_3 is not null then b.procedure_code_3 end) as distinct_vals,
        max(case when b.procedure_code_3 is not null and term.icd_10_pcs is not null then 1 else 0 end) as has_valid,
        max(case when b.procedure_code_3 is not null and term.icd_10_pcs is null then 1 else 0 end) as has_invalid,
        max(case when b.procedure_code_3 is null then 1 else 0 end) as has_null
    from base b
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_3 = term.icd_10_pcs
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
)

select * from drg_per_claim
union all select * from proc1_per_claim
union all select * from proc2_per_claim
union all select * from proc3_per_claim
