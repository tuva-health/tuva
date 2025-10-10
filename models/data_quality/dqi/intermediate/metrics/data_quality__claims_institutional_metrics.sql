{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated institutional (excluding inpatient-only metrics) terminology metrics
at data_source/payer/plan grain.
*/

with bill_type as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    )
    , per_claim as (
        select
            b.data_source,
            b.payer,
            {{ quote_column('plan') }} as plan,
            b.claim_id,
            count(distinct case when b.bill_type_code is not null then b.bill_type_code end) as distinct_vals,
            max(case when b.bill_type_code is not null and bt.bill_type_code is not null then 1 else 0 end) as has_valid,
            max(case when b.bill_type_code is not null and bt.bill_type_code is null then 1 else 0 end) as has_invalid,
            max(case when b.bill_type_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__bill_type') }} bt on b.bill_type_code = bt.bill_type_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:BILL_TYPE_CODE' as metric_id,
        'Bill Type Code (Institutional)'      as metric_name,
        'institutional'                       as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

hcpcs_outpatient as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_outpatient:HCPCS_CODE' as metric_id,
        'HCPCS Code (Institutional Outpatient)'      as metric_name,
        'institutional_outpatient'                   as claim_scope,
        sum(case when term.hcpcs is not null then 1 else 0 end) as valid_n,
        sum(case when mc.hcpcs_code is not null and term.hcpcs is null then 1 else 0 end) as invalid_n,
        sum(case when mc.hcpcs_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Institutional outpatient claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__hcpcs_level_2') }} term on mc.hcpcs_code = term.hcpcs
    where mc.claim_type = 'institutional' and {{ substring('mc.bill_type_code', 1, 2) }} != '11'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

revenue_center as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:REVENUE_CENTER_CODE' as metric_id,
        'Revenue Center Code (Institutional)'      as metric_name,
        'institutional'                             as claim_scope,
        sum(case when term.revenue_center_code is not null then 1 else 0 end) as valid_n,
        sum(case when mc.revenue_center_code is not null and term.revenue_center_code is null then 1 else 0 end) as invalid_n,
        sum(case when mc.revenue_center_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Institutional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__revenue_center') }} term on mc.revenue_center_code = term.revenue_center_code
    where mc.claim_type = 'institutional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

inst_billing_npi as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.billing_npi is not null then b.billing_npi end) as distinct_vals,
            max(case when b.billing_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
            max(case when b.billing_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
            max(case when b.billing_npi is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__provider') }} prov on b.billing_npi = prov.npi
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:BILLING_NPI' as metric_id,
        'Billing NPI (Institutional)'      as metric_name,
        'institutional'                    as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_rendering_npi as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.rendering_npi is not null then b.rendering_npi end) as distinct_vals,
            max(case when b.rendering_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
            max(case when b.rendering_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
            max(case when b.rendering_npi is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__provider') }} prov on b.rendering_npi = prov.npi
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:RENDERING_NPI' as metric_id,
        'Rendering NPI (Institutional)'      as metric_name,
        'institutional'                      as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_facility_npi as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.facility_npi is not null then b.facility_npi end) as distinct_vals,
            max(case when b.facility_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
            max(case when b.facility_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
            max(case when b.facility_npi is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__provider') }} prov on b.facility_npi = prov.npi
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:FACILITY_NPI' as metric_id,
        'Facility NPI (Institutional)'      as metric_name,
        'institutional'                     as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_dx1 as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.diagnosis_code_1 is not null then b.diagnosis_code_1 end) as distinct_vals,
            max(case when b.diagnosis_code_1 is not null and term.icd_10_cm is not null then 1 else 0 end) as has_valid,
            max(case when b.diagnosis_code_1 is not null and term.icd_10_cm is null then 1 else 0 end) as has_invalid,
            max(case when b.diagnosis_code_1 is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__icd_10_cm') }} term on b.diagnosis_code_1 = term.icd_10_cm
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_1' as metric_id,
        'Diagnosis Code 1 (Institutional)'      as metric_name,
        'institutional'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_dx2 as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.diagnosis_code_2 is not null then b.diagnosis_code_2 end) as distinct_vals,
            max(case when b.diagnosis_code_2 is not null and term.icd_10_cm is not null then 1 else 0 end) as has_valid,
            max(case when b.diagnosis_code_2 is not null and term.icd_10_cm is null then 1 else 0 end) as has_invalid,
            max(case when b.diagnosis_code_2 is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__icd_10_cm') }} term on b.diagnosis_code_2 = term.icd_10_cm
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_2' as metric_id,
        'Diagnosis Code 2 (Institutional)'      as metric_name,
        'institutional'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_dx3 as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.diagnosis_code_3 is not null then b.diagnosis_code_3 end) as distinct_vals,
            max(case when b.diagnosis_code_3 is not null and term.icd_10_cm is not null then 1 else 0 end) as has_valid,
            max(case when b.diagnosis_code_3 is not null and term.icd_10_cm is null then 1 else 0 end) as has_invalid,
            max(case when b.diagnosis_code_3 is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__icd_10_cm') }} term on b.diagnosis_code_3 = term.icd_10_cm
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_3' as metric_id,
        'Diagnosis Code 3 (Institutional)'      as metric_name,
        'institutional'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_admit_source as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.admit_source_code is not null then b.admit_source_code end) as distinct_vals,
            max(case when b.admit_source_code is not null and term.admit_source_code is not null then 1 else 0 end) as has_valid,
            max(case when b.admit_source_code is not null and term.admit_source_code is null then 1 else 0 end) as has_invalid,
            max(case when b.admit_source_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__admit_source') }} term on b.admit_source_code = term.admit_source_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:ADMIT_SOURCE_CODE' as metric_id,
        'Admit Source Code (Institutional)'      as metric_name,
        'institutional'                          as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_admit_type as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.admit_type_code is not null then b.admit_type_code end) as distinct_vals,
            max(case when b.admit_type_code is not null and term.admit_type_code is not null then 1 else 0 end) as has_valid,
            max(case when b.admit_type_code is not null and term.admit_type_code is null then 1 else 0 end) as has_invalid,
            max(case when b.admit_type_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__admit_type') }} term on b.admit_type_code = term.admit_type_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:ADMIT_TYPE_CODE' as metric_id,
        'Admit Type Code (Institutional)'      as metric_name,
        'institutional'                        as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_discharge_disposition as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
            count(distinct case when b.discharge_disposition_code is not null then b.discharge_disposition_code end) as distinct_vals,
            max(case when b.discharge_disposition_code is not null and term.discharge_disposition_code is not null then 1 else 0 end) as has_valid,
            max(case when b.discharge_disposition_code is not null and term.discharge_disposition_code is null then 1 else 0 end) as has_invalid,
            max(case when b.discharge_disposition_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__discharge_disposition') }} term on b.discharge_disposition_code = term.discharge_disposition_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source, payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DISCHARGE_DISPOSITION_CODE' as metric_id,
        'Discharge Disposition (Institutional)'           as metric_name,
        'institutional'                                   as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
)

select * from bill_type
union all select * from hcpcs_outpatient
union all select * from revenue_center
union all select * from inst_billing_npi
union all select * from inst_rendering_npi
union all select * from inst_facility_npi
union all select * from inst_dx1
union all select * from inst_dx2
union all select * from inst_dx3
union all select * from inst_admit_source
union all select * from inst_admit_type
union all select * from inst_discharge_disposition
