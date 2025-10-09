{{ config(
    materialized='table',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    )
    and
    (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Produces non-PHI, per-data_source metrics for key terminology validations.
Metrics are derived from existing DQI atomic checks where possible, and
mirrored directly from input layer data where necessary to capture the
correct denominator scopes.

Metric IDs:
 - claims:institutional_inpatient:DRG_CODE
 - claims:institutional:REVENUE_CENTER_CODE
 - claims:institutional:BILL_TYPE_CODE
 - claims:professional:HCPCS_CODE
 - claims:institutional_outpatient:HCPCS_CODE

Definitions:
 - valid = non-null value that joins to the respective terminology table
 - invalid = non-null value that does not join to terminology
 - null = null value
 - multiple = more than one distinct value at the expected grain (only applies for certain fields)
*/

with drg as (
    /* Inpatient DRG by claim_id with multiple detection; includes payer and plan */
    with base as (
        select *
        from {{ ref('medical_claim') }}
        where claim_type = 'institutional'
          and {{ substring('bill_type_code', 1, 2) }} = '11'
    ),
    per_claim as (
        select
            b.data_source,
            b.payer,
            {{ quote_column('plan') }} as plan,
            b.claim_id,
            count(distinct case when b.drg_code is not null then b.drg_code end) as distinct_codes,
            max(case when b.drg_code is not null and (
                    (b.drg_code_type = 'ms-drg' and ms.ms_drg_code is not null) or
                    (b.drg_code_type = 'apr-drg' and apr.apr_drg_code is not null)
                ) then 1 else 0 end) as has_valid,
            max(case when b.drg_code is not null and (
                    (b.drg_code_type = 'ms-drg' and ms.ms_drg_code is null) or
                    (b.drg_code_type = 'apr-drg' and apr.apr_drg_code is null)
                ) then 1 else 0 end) as has_invalid,
            max(case when b.drg_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__ms_drg') }} ms on b.drg_code_type = 'ms-drg' and b.drg_code = ms.ms_drg_code
        left join {{ ref('terminology__apr_drg') }} apr on b.drg_code_type = 'apr-drg' and b.drg_code = apr.apr_drg_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:DRG_CODE' as metric_id,
        'DRG Code (Inpatient)' as metric_name,
        'institutional_inpatient' as claim_scope,
        sum(case when distinct_codes > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_codes > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_codes > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_codes > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

-- Aggregate all claims atomic checks to ensure complete coverage
-- This captures every atomic check from the DQI claims union, even those
-- that do not join to terminology (dates, amounts, identifiers, etc.).
-- We exclude fields already represented explicitly above to avoid duplicates
all_atomic_raw as (
    select
        d.data_source,
        d.claim_type,
        d.field_name,
        sum(case when d.bucket_name = 'valid' then 1 else 0 end)   as valid_n,
        sum(case when d.bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when d.bucket_name = 'null' then 1 else 0 end)    as null_n,
        sum(case when d.bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        max(case when d.drill_down_key like '%Claim Line Number%' then 1 else 0 end) as any_line_level
    from {{ ref('data_quality__data_quality_claims_detail_union') }} d
    where not (
        -- Exclude metrics that are already included explicitly in this model
        (d.claim_type = 'institutional_inpatient' and d.field_name in ('DRG_CODE', 'PROCEDURE_CODE_1', 'PROCEDURE_CODE_2', 'PROCEDURE_CODE_3'))
        or (d.claim_type = 'institutional' and d.field_name in (
            'DRG_CODE', 'BILL_TYPE_CODE', 'REVENUE_CENTER_CODE',
            'BILLING_NPI', 'RENDERING_NPI', 'FACILITY_NPI',
            'DIAGNOSIS_CODE_1', 'DIAGNOSIS_CODE_2', 'DIAGNOSIS_CODE_3',
            'ADMIT_SOURCE_CODE', 'ADMIT_TYPE_CODE', 'DISCHARGE_DISPOSITION_CODE'
        ))
        or (d.claim_type = 'professional' and d.field_name in (
            'HCPCS_CODE', 'PLACE_OF_SERVICE_CODE',
            'BILLING_NPI', 'RENDERING_NPI', 'FACILITY_NPI',
            'DIAGNOSIS_CODE_1', 'DIAGNOSIS_CODE_2', 'DIAGNOSIS_CODE_3'
        ))
        or (d.claim_type = 'institutional_outpatient' and d.field_name in ('HCPCS_CODE'))
        or (d.claim_type = 'pharmacy' and d.field_name in (
            'NDC_CODE', 'DISPENSING_PROVIDER_NPI', 'PRESCRIBING_PROVIDER_NPI'
        ))
        or (d.claim_type = 'eligibility' and d.field_name in (
            'GENDER', 'RACE', 'PAYER_TYPE', 'MEDICARE_STATUS_CODE', 'DUAL_STATUS_CODE', 'ORIGINAL_REASON_ENTITLEMENT_CODE'
        ))
        or (d.claim_type = 'medical' and d.field_name in ('CLAIM_TYPE'))
    )
    group by d.data_source, d.claim_type, d.field_name
),

all_atomic as (
    select
        data_source,
        null as payer,
        null as plan,
        {{ concat_custom(["'claims:'", "claim_type", "':'", "field_name"]) }} as metric_id,
        field_name as metric_name,
        claim_type as claim_scope,
        valid_n,
        invalid_n,
        null_n,
        multiple_n,
        denominator_n,
        case
            when claim_type = 'eligibility' then 'Eligibility records'
            when claim_type = 'pharmacy' then 'Pharmacy claim lines'
            when claim_type = 'professional' then 'Professional claim lines'
            when claim_type = 'medical' then 'All medical claims'
            when claim_type = 'institutional' and any_line_level = 1 then 'Institutional claim lines'
            when claim_type = 'institutional' then 'Institutional claims'
            else 'Records evaluated'
        end as denominator_desc
    from all_atomic_raw
),

bill_type as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source,
            b.payer,
            {{ quote_column('plan') }} as plan,
            b.claim_id,
            count(distinct case when b.bill_type_code is not null then b.bill_type_code end) as distinct_codes,
            max(case when b.bill_type_code is not null and bt.bill_type_code is not null then 1 else 0 end) as has_valid,
            max(case when b.bill_type_code is not null and bt.bill_type_code is null then 1 else 0 end) as has_invalid,
            max(case when b.bill_type_code is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__bill_type') }} bt on b.bill_type_code = bt.bill_type_code
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional:BILL_TYPE_CODE' as metric_id,
        'Bill Type Code (Institutional)' as metric_name,
        'institutional' as claim_scope,
        sum(case when distinct_codes > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_codes > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_codes > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_codes > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

revenue_center as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional:REVENUE_CENTER_CODE' as metric_id,
        'Revenue Center Code (Institutional)'      as metric_name,
        'institutional'                             as claim_scope,
        sum(case when term.revenue_center_code is not null then 1 else 0 end)     as valid_n,
        sum(case when mc.revenue_center_code is not null and term.revenue_center_code is null then 1 else 0 end)   as invalid_n,
        sum(case when mc.revenue_center_code is null then 1 else 0 end)      as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Institutional claim lines' as denominator_desc
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__revenue_center') }} term on mc.revenue_center_code = term.revenue_center_code
    where mc.claim_type = 'institutional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

-- HCPCS (Professional): mirror data_quality__claim_hcpcs_code but restrict to professional claim_type for denominator scope
hcpcs_professional as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:HCPCS_CODE'         as metric_id,
        'HCPCS Code (Professional)'              as metric_name,
        'professional'                           as claim_scope,
        sum(case when term.hcpcs is not null then 1 else 0 end)                as valid_n,
        sum(case when mc.hcpcs_code is not null and term.hcpcs is null then 1 else 0 end) as invalid_n,
        sum(case when mc.hcpcs_code is null then 1 else 0 end)                 as null_n,
        0                                                                      as multiple_n,
        count(*)                                                               as denominator_n,
        'Professional claim lines'                                             as denominator_desc
    from {{ ref('medical_claim') }} as mc
    left join {{ ref('terminology__hcpcs_level_2') }} as term
        on mc.hcpcs_code = term.hcpcs
    where mc.claim_type = 'professional'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

-- HCPCS (Institutional Outpatient): institutional claim_type excluding inpatient (bill_type prefix '11')
hcpcs_institutional_outpatient as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional_outpatient:HCPCS_CODE' as metric_id,
        'HCPCS Code (Institutional Outpatient)'      as metric_name,
        'institutional_outpatient'                   as claim_scope,
        sum(case when term.hcpcs is not null then 1 else 0 end)                as valid_n,
        sum(case when mc.hcpcs_code is not null and term.hcpcs is null then 1 else 0 end) as invalid_n,
        sum(case when mc.hcpcs_code is null then 1 else 0 end)                 as null_n,
        0                                                                      as multiple_n,
        count(*)                                                               as denominator_n,
        'Institutional outpatient claim lines'                                 as denominator_desc
    from {{ ref('medical_claim') }} as mc
    left join {{ ref('terminology__hcpcs_level_2') }} as term
        on mc.hcpcs_code = term.hcpcs
    where mc.claim_type = 'institutional'
      and {{ substring('mc.bill_type_code', 1, 2) }} != '11'
    group by mc.data_source, mc.payer, {{ quote_column('plan') }}
),

-- Claim Type (All Medical Claims)
claim_type as (
    select
        m.data_source,
        m.payer,
        {{ quote_column('plan') }} as plan,
        'claims:medical:CLAIM_TYPE'            as metric_id,
        'Claim Type (Medical)'                 as metric_name,
        'medical'                              as claim_scope,
        sum(case when term.claim_type is not null then 1 else 0 end)     as valid_n,
        sum(case when m.claim_type is not null and term.claim_type is null then 1 else 0 end)   as invalid_n,
        sum(case when m.claim_type is null then 1 else 0 end)      as null_n,
        0 as multiple_n,
        count(*)                                                   as denominator_n,
        'All medical claims'                                       as denominator_desc
    from {{ ref('medical_claim') }} as m
    left join {{ ref('terminology__claim_type') }} as term on m.claim_type = term.claim_type
    group by m.data_source, m.payer, {{ quote_column('plan') }}
),

-- Professional POS
pos_professional as (
    select
        m.data_source,
        m.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:PLACE_OF_SERVICE_CODE' as metric_id,
        'Place of Service (Professional)'           as metric_name,
        'professional'                               as claim_scope,
        sum(case when term.place_of_service_code is not null then 1 else 0 end)     as valid_n,
        sum(case when m.place_of_service_code is not null and term.place_of_service_code is null then 1 else 0 end)   as invalid_n,
        sum(case when m.place_of_service_code is null then 1 else 0 end)      as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('medical_claim') }} as m
    left join {{ ref('terminology__place_of_service') }} as term on m.place_of_service_code = term.place_of_service_code
    where m.claim_type = 'professional'
    group by m.data_source, m.payer, {{ quote_column('plan') }}
),

-- Professional NPIs
prof_billing_npi as (
    select
        m.data_source,
        m.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:BILLING_NPI'      as metric_id,
        'Billing NPI (Professional)'           as metric_name,
        'professional'                         as claim_scope,
        sum(case when term.npi is not null then 1 else 0 end) as valid_n,
        sum(case when m.billing_npi is not null and term.npi is null then 1 else 0 end) as invalid_n,
        sum(case when m.billing_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('medical_claim') }} as m
    left join {{ ref('terminology__provider') }} as term on m.billing_npi = term.npi
    where m.claim_type = 'professional'
    group by m.data_source, m.payer, {{ quote_column('plan') }}
),

prof_rendering_npi as (
    select
        m.data_source,
        m.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:RENDERING_NPI'     as metric_id,
        'Rendering NPI (Professional)'          as metric_name,
        'professional'                          as claim_scope,
        sum(case when term.npi is not null then 1 else 0 end) as valid_n,
        sum(case when m.rendering_npi is not null and term.npi is null then 1 else 0 end) as invalid_n,
        sum(case when m.rendering_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('medical_claim') }} as m
    left join {{ ref('terminology__provider') }} as term on m.rendering_npi = term.npi
    where m.claim_type = 'professional'
    group by m.data_source, m.payer, {{ quote_column('plan') }}
),

prof_facility_npi as (
    select
        m.data_source,
        m.payer,
        {{ quote_column('plan') }} as plan,
        'claims:professional:FACILITY_NPI'      as metric_id,
        'Facility NPI (Professional)'           as metric_name,
        'professional'                          as claim_scope,
        sum(case when term.npi is not null then 1 else 0 end) as valid_n,
        sum(case when m.facility_npi is not null and term.npi is null then 1 else 0 end) as invalid_n,
        sum(case when m.facility_npi is null then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('medical_claim') }} as m
    left join {{ ref('terminology__provider') }} as term on m.facility_npi = term.npi
    where m.claim_type = 'professional'
    group by m.data_source, m.payer, {{ quote_column('plan') }}
),

-- Institutional NPIs (claim-level, includes multiple detection)
inst_billing_npi as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source,
            b.payer,
            {{ quote_column('plan') }} as plan,
            b.claim_id,
            count(distinct case when b.billing_npi is not null then b.billing_npi end) as distinct_vals,
            max(case when b.billing_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
            max(case when b.billing_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
            max(case when b.billing_npi is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__provider') }} prov on b.billing_npi = prov.npi
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional:BILLING_NPI'      as metric_id,
        'Billing NPI (Institutional)'           as metric_name,
        'institutional'                         as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_rendering_npi as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source,
            b.payer,
            {{ quote_column('plan') }} as plan,
            b.claim_id,
            count(distinct case when b.rendering_npi is not null then b.rendering_npi end) as distinct_vals,
            max(case when b.rendering_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
            max(case when b.rendering_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
            max(case when b.rendering_npi is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__provider') }} prov on b.rendering_npi = prov.npi
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional:RENDERING_NPI'     as metric_id,
        'Rendering NPI (Institutional)'          as metric_name,
        'institutional'                          as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

inst_facility_npi as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
    ), per_claim as (
        select
            b.data_source,
            b.payer,
            {{ quote_column('plan') }} as plan,
            b.claim_id,
            count(distinct case when b.facility_npi is not null then b.facility_npi end) as distinct_vals,
            max(case when b.facility_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
            max(case when b.facility_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
            max(case when b.facility_npi is null then 1 else 0 end) as has_null
        from base b
        left join {{ ref('terminology__provider') }} prov on b.facility_npi = prov.npi
        group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
    )
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        'claims:institutional:FACILITY_NPI'      as metric_id,
        'Facility NPI (Institutional)'           as metric_name,
        'institutional'                          as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from per_claim
    group by data_source, payer, {{ quote_column('plan') }}
),

-- Diagnosis Codes (Professional 1-3)
prof_dx1 as (
    select m.data_source,
        'claims:professional:DIAGNOSIS_CODE_1'   as metric_id,
        'Diagnosis Code 1 (Professional)'        as metric_name,
        'professional'                           as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('data_quality__professional_diagnosis_code_1') }} as m
    group by m.data_source
),
prof_dx2 as (
    select m.data_source,
        'claims:professional:DIAGNOSIS_CODE_2'   as metric_id,
        'Diagnosis Code 2 (Professional)'        as metric_name,
        'professional'                           as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('data_quality__professional_diagnosis_code_2') }} as m
    group by m.data_source
),
prof_dx3 as (
    select m.data_source,
        'claims:professional:DIAGNOSIS_CODE_3'   as metric_id,
        'Diagnosis Code 3 (Professional)'        as metric_name,
        'professional'                           as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Professional claim lines' as denominator_desc
    from {{ ref('data_quality__professional_diagnosis_code_3') }} as m
    group by m.data_source
),

-- Diagnosis Codes (Institutional 1-3, claim-level with multiple)
inst_dx1 as (
    select m.data_source,
        'claims:institutional:DIAGNOSIS_CODE_1'  as metric_id,
        'Diagnosis Code 1 (Institutional)'       as metric_name,
        'institutional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from {{ ref('data_quality__institutional_diagnosis_code_1') }} as m
    group by m.data_source
),
inst_dx2 as (
    select m.data_source,
        'claims:institutional:DIAGNOSIS_CODE_2'  as metric_id,
        'Diagnosis Code 2 (Institutional)'       as metric_name,
        'institutional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from {{ ref('data_quality__institutional_diagnosis_code_2') }} as m
    group by m.data_source
),
inst_dx3 as (
    select m.data_source,
        'claims:institutional:DIAGNOSIS_CODE_3'  as metric_id,
        'Diagnosis Code 3 (Institutional)'       as metric_name,
        'institutional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from {{ ref('data_quality__institutional_diagnosis_code_3') }} as m
    group by m.data_source
),

-- Procedure Codes (Institutional Inpatient 1-3, claim-level with multiple)
inst_proc1 as (
    select m.data_source,
        'claims:institutional_inpatient:PROCEDURE_CODE_1' as metric_id,
        'Procedure Code 1 (Inpatient)'                    as metric_name,
        'institutional_inpatient'                         as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from {{ ref('data_quality__institutional_procedure_code_1') }} as m
    group by m.data_source
),
inst_proc2 as (
    select m.data_source,
        'claims:institutional_inpatient:PROCEDURE_CODE_2' as metric_id,
        'Procedure Code 2 (Inpatient)'                    as metric_name,
        'institutional_inpatient'                         as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from {{ ref('data_quality__institutional_procedure_code_2') }} as m
    group by m.data_source
),
inst_proc3 as (
    select m.data_source,
        'claims:institutional_inpatient:PROCEDURE_CODE_3' as metric_id,
        'Procedure Code 3 (Inpatient)'                    as metric_name,
        'institutional_inpatient'                         as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional inpatient claims' as denominator_desc
    from {{ ref('data_quality__institutional_procedure_code_3') }} as m
    group by m.data_source
),

-- Institutional admit/discharge codes
inst_admit_source as (
    select m.data_source,
        'claims:institutional:ADMIT_SOURCE_CODE'  as metric_id,
        'Admit Source Code (Institutional)'       as metric_name,
        'institutional'                           as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from {{ ref('data_quality__institutional_admit_source_code') }} as m
    group by m.data_source
),
inst_admit_type as (
    select m.data_source,
        'claims:institutional:ADMIT_TYPE_CODE'    as metric_id,
        'Admit Type Code (Institutional)'         as metric_name,
        'institutional'                           as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from {{ ref('data_quality__institutional_admit_type_code') }} as m
    group by m.data_source
),
inst_discharge_disposition as (
    select m.data_source,
        'claims:institutional:DISCHARGE_DISPOSITION_CODE' as metric_id,
        'Discharge Disposition (Institutional)'           as metric_name,
        'institutional'                                   as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from {{ ref('data_quality__institutional_discharge_disposition_code') }} as m
    group by m.data_source
),

-- Pharmacy NDC and NPIs
pharm_ndc as (
    select m.data_source,
        'claims:pharmacy:NDC_CODE'             as metric_id,
        'NDC Code (Pharmacy)'                  as metric_name,
        'pharmacy'                              as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Pharmacy claim lines' as denominator_desc
    from {{ ref('data_quality__pharmacy_ndc_code') }} as m
    group by m.data_source
),
pharm_dispensing_npi as (
    select m.data_source,
        'claims:pharmacy:DISPENSING_PROVIDER_NPI' as metric_id,
        'Dispensing Provider NPI (Pharmacy)'      as metric_name,
        'pharmacy'                                as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Pharmacy claim lines' as denominator_desc
    from {{ ref('data_quality__pharmacy_dispensing_provider_npi') }} as m
    group by m.data_source
),
pharm_prescribing_npi as (
    select m.data_source,
        'claims:pharmacy:PRESCRIBING_PROVIDER_NPI' as metric_id,
        'Prescribing Provider NPI (Pharmacy)'      as metric_name,
        'pharmacy'                                 as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Pharmacy claim lines' as denominator_desc
    from {{ ref('data_quality__pharmacy_prescribing_provider_npi') }} as m
    group by m.data_source
),

-- Eligibility terminology checks
elig_gender as (
    select m.data_source,
        'claims:eligibility:GENDER'            as metric_id,
        'Gender (Eligibility)'                 as metric_name,
        'eligibility'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('data_quality__eligibility_gender') }} as m
    group by m.data_source
),
elig_race as (
    select m.data_source,
        'claims:eligibility:RACE'              as metric_id,
        'Race (Eligibility)'                   as metric_name,
        'eligibility'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('data_quality__eligibility_race') }} as m
    group by m.data_source
),
elig_payer_type as (
    select m.data_source,
        'claims:eligibility:PAYER_TYPE'        as metric_id,
        'Payer Type (Eligibility)'             as metric_name,
        'eligibility'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('data_quality__eligibility_payer_type') }} as m
    group by m.data_source
),
elig_medicare_status as (
    select m.data_source,
        'claims:eligibility:MEDICARE_STATUS_CODE' as metric_id,
        'Medicare Status Code (Eligibility)'      as metric_name,
        'eligibility'                              as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('data_quality__eligibility_medicare_status_code') }} as m
    group by m.data_source
),
elig_dual_status as (
    select m.data_source,
        'claims:eligibility:DUAL_STATUS_CODE'  as metric_id,
        'Dual Status Code (Eligibility)'       as metric_name,
        'eligibility'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('data_quality__eligibility_dual_status_code') }} as m
    group by m.data_source
),
elig_orec as (
    select m.data_source,
        'claims:eligibility:ORIGINAL_REASON_ENTITLEMENT_CODE' as metric_id,
        'Original Reason Entitlement Code (Eligibility)'       as metric_name,
        'eligibility'                                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end) as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end) as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        'Eligibility records' as denominator_desc
    from {{ ref('data_quality__eligibility_original_reason_entitlement_code') }} as m
    group by m.data_source
),

unioned as (
    select * from drg
    union all
    select * from bill_type
    union all
    select * from revenue_center
    union all
    select * from hcpcs_professional
    union all
    select * from hcpcs_institutional_outpatient
    union all
    select * from claim_type
    union all
    select * from pos_professional
    union all
    select * from prof_billing_npi
    union all
    select * from prof_rendering_npi
    union all
    select * from prof_facility_npi
    union all
    select * from inst_billing_npi
    union all
    select * from inst_rendering_npi
    union all
    select * from inst_facility_npi
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from prof_dx1
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from prof_dx2
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from prof_dx3
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_dx1
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_dx2
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_dx3
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_proc1
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_proc2
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_proc3
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_admit_source
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_admit_type
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from inst_discharge_disposition
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from pharm_ndc
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from pharm_dispensing_npi
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from pharm_prescribing_npi
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from elig_gender
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from elig_race
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from elig_payer_type
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from elig_medicare_status
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from elig_dual_status
    union all
    select data_source, null as payer, null as plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from elig_orec
    union all
    -- Append all remaining atomic checks not already represented above
    select data_source, payer, plan, metric_id, metric_name, claim_scope, valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc from all_atomic
)

select
    data_source,
    payer,
    {{ quote_column('plan') }} as plan,
    metric_id,
    metric_name,
    claim_scope,
    denominator_n,
    valid_n,
    invalid_n,
    null_n,
    multiple_n,
    case when denominator_n > 0 then 1.0 * valid_n / denominator_n else null end as valid_pct,
    case
        when metric_id = 'claims:institutional_outpatient:HCPCS_CODE' then 0.80
        else 0.97
    end as threshold,
    case when denominator_n > 0 and (1.0 * valid_n / denominator_n) >= case when metric_id = 'claims:institutional_outpatient:HCPCS_CODE' then 0.80 else 0.97 end and multiple_n = 0 then true else false end as pass_flag,
    denominator_desc,
    '{{ var('tuva_last_run') }}' as tuva_last_run
from unioned
 
