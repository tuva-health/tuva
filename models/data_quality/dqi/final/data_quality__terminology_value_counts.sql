{{ config(
    materialized='table',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    )
    and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Row-level value distributions for terminology validations by data_source, payer, plan.
Only includes public terminology values (codes) and counts; no PHI.

Columns:
 - data_source, payer, plan, metric_id, claim_scope, bucket_name ('valid'|'invalid'), field_value (code|desc for valid; code| for invalid), frequency

Metrics covered:
 - DRG inpatient, Bill Type (inst), Revenue Center (inst)
 - HCPCS (prof, inst outpatient), POS (prof), Claim Type (medical)
*/

with drg as (
    with base as (
        select * from {{ ref('medical_claim') }}
        where claim_type = 'institutional' and {{ substring('bill_type_code',1,2) }} = '11'
    )
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:DRG_CODE' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when (b.drg_code is not null and ((b.drg_code_type='ms-drg' and ms.ms_drg_code is not null) or (b.drg_code_type='apr-drg' and apr.apr_drg_code is not null))) then 'valid'
             when b.drg_code is not null then 'invalid'
        end as bucket_name,
        case when (b.drg_code_type='ms-drg' and ms.ms_drg_code is not null) then concat(b.drg_code,'|',coalesce(ms.ms_drg_description,''))
             when (b.drg_code_type='apr-drg' and apr.apr_drg_code is not null) then concat(b.drg_code,'|',coalesce(apr.apr_drg_description,''))
             else concat(b.drg_code,'|') end as field_value
    from base b
    left join {{ ref('terminology__ms_drg') }} ms on b.drg_code_type = 'ms-drg' and b.drg_code = ms.ms_drg_code
    left join {{ ref('terminology__apr_drg') }} apr on b.drg_code_type = 'apr-drg' and b.drg_code = apr.apr_drg_code
    where b.drg_code is not null
),

bill_type as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:BILL_TYPE_CODE' as metric_id,
        'institutional' as claim_scope,
        case when bt.bill_type_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when bt.bill_type_code is not null then concat(b.bill_type_code,'|',coalesce(bt.bill_type_description,'')) else concat(b.bill_type_code,'|') end as field_value
    from {{ ref('medical_claim') }} b
    left join {{ ref('terminology__bill_type') }} bt on b.bill_type_code = bt.bill_type_code
    where b.claim_type = 'institutional' and b.bill_type_code is not null
),

revenue_center as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:REVENUE_CENTER_CODE' as metric_id,
        'institutional' as claim_scope,
        case when term.revenue_center_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.revenue_center_code is not null then concat(mc.revenue_center_code,'|',coalesce(term.revenue_center_description,'')) else concat(mc.revenue_center_code,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__revenue_center') }} term on mc.revenue_center_code = term.revenue_center_code
    where mc.claim_type = 'institutional' and mc.revenue_center_code is not null
),

hcpcs_professional as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:HCPCS_CODE' as metric_id,
        'professional' as claim_scope,
        case when term.hcpcs is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.hcpcs is not null then concat(mc.hcpcs_code,'|',coalesce(term.short_description,'')) else concat(mc.hcpcs_code,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__hcpcs_level_2') }} term on mc.hcpcs_code = term.hcpcs
    where mc.claim_type = 'professional' and mc.hcpcs_code is not null
),

hcpcs_inst_out as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_outpatient:HCPCS_CODE' as metric_id,
        'institutional_outpatient' as claim_scope,
        case when term.hcpcs is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.hcpcs is not null then concat(mc.hcpcs_code,'|',coalesce(term.short_description,'')) else concat(mc.hcpcs_code,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__hcpcs_level_2') }} term on mc.hcpcs_code = term.hcpcs
    where mc.claim_type = 'institutional' and {{ substring('mc.bill_type_code',1,2) }} != '11' and mc.hcpcs_code is not null
),

pos_professional as (
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:PLACE_OF_SERVICE_CODE' as metric_id,
        'professional' as claim_scope,
        case when term.place_of_service_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.place_of_service_code is not null then concat(m.place_of_service_code,'|',coalesce(term.place_of_service_description,'')) else concat(m.place_of_service_code,'|') end as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__place_of_service') }} term on m.place_of_service_code = term.place_of_service_code
    where m.claim_type = 'professional' and m.place_of_service_code is not null
),

claim_type as (
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:medical:CLAIM_TYPE' as metric_id,
        'medical' as claim_scope,
        case when term.claim_type is not null then 'valid' else 'invalid' end as bucket_name,
        m.claim_type || '|' as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__claim_type') }} term on m.claim_type = term.claim_type
    where m.claim_type is not null
),

unioned as (
    select * from drg
    union all select * from bill_type
    union all select * from revenue_center
    union all select * from hcpcs_professional
    union all select * from hcpcs_inst_out
    union all select * from pos_professional
    union all select * from claim_type
)
select data_source, payer, {{ quote_column('plan') }} as plan, metric_id, claim_scope, bucket_name, field_value, count(*) as frequency
from unioned
group by data_source, payer, {{ quote_column('plan') }}, metric_id, claim_scope, bucket_name, field_value

