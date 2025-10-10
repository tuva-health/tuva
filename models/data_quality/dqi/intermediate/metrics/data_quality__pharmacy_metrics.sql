{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated pharmacy terminology metrics at data_source/payer/plan grain.
*/

with ndc as (
    select
        p.data_source, p.payer, {{ quote_column('plan') }} as plan,
        'claims:pharmacy:NDC_CODE' as metric_id,
        'NDC Code (Pharmacy)'      as metric_name,
        'pharmacy'                 as claim_scope,
        sum(case when term.ndc is not null then 1 else 0 end) as valid_n,
        sum(case when p.ndc_code is not null and term.ndc is null then 1 else 0 end) as invalid_n,
        sum(case when p.ndc_code is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Pharmacy claim lines' as denominator_desc
    from {{ ref('pharmacy_claim') }} p
    left join {{ ref('terminology__ndc') }} term on p.ndc_code = term.ndc
    group by p.data_source, p.payer, {{ quote_column('plan') }}
),

dispensing_npi as (
    select
        p.data_source, p.payer, {{ quote_column('plan') }} as plan,
        'claims:pharmacy:DISPENSING_PROVIDER_NPI' as metric_id,
        'Dispensing Provider NPI (Pharmacy)'      as metric_name,
        'pharmacy'                                 as claim_scope,
        sum(case when prov.npi is not null then 1 else 0 end) as valid_n,
        sum(case when p.dispensing_provider_npi is not null and prov.npi is null then 1 else 0 end) as invalid_n,
        sum(case when p.dispensing_provider_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Pharmacy claim lines' as denominator_desc
    from {{ ref('pharmacy_claim') }} p
    left join {{ ref('terminology__provider') }} prov on p.dispensing_provider_npi = prov.npi
    group by p.data_source, p.payer, {{ quote_column('plan') }}
),

prescribing_npi as (
    select
        p.data_source, p.payer, {{ quote_column('plan') }} as plan,
        'claims:pharmacy:PRESCRIBING_PROVIDER_NPI' as metric_id,
        'Prescribing Provider NPI (Pharmacy)'      as metric_name,
        'pharmacy'                                  as claim_scope,
        sum(case when prov.npi is not null then 1 else 0 end) as valid_n,
        sum(case when p.prescribing_provider_npi is not null and prov.npi is null then 1 else 0 end) as invalid_n,
        sum(case when p.prescribing_provider_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Pharmacy claim lines' as denominator_desc
    from {{ ref('pharmacy_claim') }} p
    left join {{ ref('terminology__provider') }} prov on p.prescribing_provider_npi = prov.npi
    group by p.data_source, p.payer, {{ quote_column('plan') }}
)

select * from ndc
union all select * from dispensing_npi
union all select * from prescribing_npi

