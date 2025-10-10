{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated professional-claims terminology metrics at data_source/payer/plan grain.
Outputs columns matching data_quality__terminology_metrics union schema:
 - data_source, payer, plan, metric_id, metric_name, claim_scope,
   valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc
*/

with hcpcs as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:HCPCS_CODE' as metric_id,
        'HCPCS Code (Professional)'      as metric_name,
        'professional'                   as claim_scope,
        sum(case when term.hcpcs is not null then 1 else 0 end) as valid_n,
        sum(case when mc.hcpcs_code is not null and term.hcpcs is null then 1 else 0 end) as invalid_n,
        sum(case when mc.hcpcs_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__hcpcs_level_2') }} term on mc.hcpcs_code = term.hcpcs
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

pos as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:PLACE_OF_SERVICE_CODE' as metric_id,
        'Place of Service (Professional)'           as metric_name,
        'professional'                               as claim_scope,
        sum(case when term.place_of_service_code is not null then 1 else 0 end) as valid_n,
        sum(case when mc.place_of_service_code is not null and term.place_of_service_code is null then 1 else 0 end) as invalid_n,
        sum(case when mc.place_of_service_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__place_of_service') }} term on mc.place_of_service_code = term.place_of_service_code
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

prof_billing_npi as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:BILLING_NPI' as metric_id,
        'Billing NPI (Professional)'      as metric_name,
        'professional'                    as claim_scope,
        sum(case when prov.npi is not null then 1 else 0 end) as valid_n,
        sum(case when mc.billing_npi is not null and prov.npi is null then 1 else 0 end) as invalid_n,
        sum(case when mc.billing_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__provider') }} prov on mc.billing_npi = prov.npi
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

prof_rendering_npi as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:RENDERING_NPI' as metric_id,
        'Rendering NPI (Professional)'      as metric_name,
        'professional'                      as claim_scope,
        sum(case when prov.npi is not null then 1 else 0 end) as valid_n,
        sum(case when mc.rendering_npi is not null and prov.npi is null then 1 else 0 end) as invalid_n,
        sum(case when mc.rendering_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__provider') }} prov on mc.rendering_npi = prov.npi
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

prof_facility_npi as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:FACILITY_NPI' as metric_id,
        'Facility NPI (Professional)'      as metric_name,
        'professional'                     as claim_scope,
        sum(case when prov.npi is not null then 1 else 0 end) as valid_n,
        sum(case when mc.facility_npi is not null and prov.npi is null then 1 else 0 end) as invalid_n,
        sum(case when mc.facility_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__provider') }} prov on mc.facility_npi = prov.npi
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

prof_dx1 as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_1' as metric_id,
        'Diagnosis Code 1 (Professional)'      as metric_name,
        'professional'                         as claim_scope,
        sum(case when term.icd_10_cm is not null then 1 else 0 end) as valid_n,
        sum(case when mc.diagnosis_code_1 is not null and term.icd_10_cm is null then 1 else 0 end) as invalid_n,
        sum(case when mc.diagnosis_code_1 is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_1 = term.icd_10_cm
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

prof_dx2 as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_2' as metric_id,
        'Diagnosis Code 2 (Professional)'      as metric_name,
        'professional'                         as claim_scope,
        sum(case when term.icd_10_cm is not null then 1 else 0 end) as valid_n,
        sum(case when mc.diagnosis_code_2 is not null and term.icd_10_cm is null then 1 else 0 end) as invalid_n,
        sum(case when mc.diagnosis_code_2 is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_2 = term.icd_10_cm
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

prof_dx3 as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_3' as metric_id,
        'Diagnosis Code 3 (Professional)'      as metric_name,
        'professional'                         as claim_scope,
        sum(case when term.icd_10_cm is not null then 1 else 0 end) as valid_n,
        sum(case when mc.diagnosis_code_3 is not null and term.icd_10_cm is null then 1 else 0 end) as invalid_n,
        sum(case when mc.diagnosis_code_3 is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_3 = term.icd_10_cm
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
)

select * from hcpcs
union all select * from pos
union all select * from prof_billing_npi
union all select * from prof_rendering_npi
union all select * from prof_facility_npi
union all select * from prof_dx1
union all select * from prof_dx2
union all select * from prof_dx3

