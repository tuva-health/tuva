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

Metrics covered (aligned with data_quality__terminology_metrics):
 - Institutional inpatient: DRG, Procedure Code 1-3
 - Institutional: Bill Type, Revenue Center, Diagnosis Code 1-3, Admit Source, Admit Type, Discharge Disposition, NPIs (Billing/Rendering/Facility)
 - Professional: HCPCS, POS, Diagnosis Code 1-3, NPIs (Billing/Rendering/Facility)
 - Institutional outpatient: HCPCS
 - Medical: Claim Type
 - Pharmacy: NDC Code, NPIs (Dispensing/Prescribing)
 - Eligibility: Gender, Race, Payer Type, Medicare Status Code, Dual Status Code, Original Reason Entitlement Code (OREC)

Note: This dataset focuses on valid/invalid value distributions to power UI “Top offenders.”
      Null/multiple buckets are surfaced in the metrics table, not here.
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

-- Inpatient ICD-10-PCS procedure codes (1-3)
proc1 as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional' and {{ substring('bill_type_code',1,2) }} = '11'
    )
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:PROCEDURE_CODE_1' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when term.icd_10_pcs is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_pcs is not null then concat(b.procedure_code_1,'|',coalesce(term.description,'')) else concat(b.procedure_code_1,'|') end as field_value
    from base b
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_1 = term.icd_10_pcs
    where b.procedure_code_1 is not null
),
proc2 as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional' and {{ substring('bill_type_code',1,2) }} = '11'
    )
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:PROCEDURE_CODE_2' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when term.icd_10_pcs is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_pcs is not null then concat(b.procedure_code_2,'|',coalesce(term.description,'')) else concat(b.procedure_code_2,'|') end as field_value
    from base b
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_2 = term.icd_10_pcs
    where b.procedure_code_2 is not null
),
proc3 as (
    with base as (
        select * from {{ ref('medical_claim') }} where claim_type = 'institutional' and {{ substring('bill_type_code',1,2) }} = '11'
    )
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional_inpatient:PROCEDURE_CODE_3' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when term.icd_10_pcs is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_pcs is not null then concat(b.procedure_code_3,'|',coalesce(term.description,'')) else concat(b.procedure_code_3,'|') end as field_value
    from base b
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_3 = term.icd_10_pcs
    where b.procedure_code_3 is not null
),

-- Diagnosis codes: Professional 1-3 and Institutional 1-3
prof_dx1 as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_1' as metric_id,
        'professional' as claim_scope,
        case when term.icd_10_cm is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_cm is not null then concat(mc.diagnosis_code_1,'|',coalesce(term.short_description,'')) else concat(mc.diagnosis_code_1,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_1 = term.icd_10_cm
    where mc.claim_type = 'professional' and mc.diagnosis_code_1 is not null
),
prof_dx2 as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_2' as metric_id,
        'professional' as claim_scope,
        case when term.icd_10_cm is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_cm is not null then concat(mc.diagnosis_code_2,'|',coalesce(term.short_description,'')) else concat(mc.diagnosis_code_2,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_2 = term.icd_10_cm
    where mc.claim_type = 'professional' and mc.diagnosis_code_2 is not null
),
prof_dx3 as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_3' as metric_id,
        'professional' as claim_scope,
        case when term.icd_10_cm is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_cm is not null then concat(mc.diagnosis_code_3,'|',coalesce(term.short_description,'')) else concat(mc.diagnosis_code_3,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_3 = term.icd_10_cm
    where mc.claim_type = 'professional' and mc.diagnosis_code_3 is not null
),
inst_dx1 as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_1' as metric_id,
        'institutional' as claim_scope,
        case when term.icd_10_cm is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_cm is not null then concat(mc.diagnosis_code_1,'|',coalesce(term.short_description,'')) else concat(mc.diagnosis_code_1,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_1 = term.icd_10_cm
    where mc.claim_type = 'institutional' and mc.diagnosis_code_1 is not null
),
inst_dx2 as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_2' as metric_id,
        'institutional' as claim_scope,
        case when term.icd_10_cm is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_cm is not null then concat(mc.diagnosis_code_2,'|',coalesce(term.short_description,'')) else concat(mc.diagnosis_code_2,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_2 = term.icd_10_cm
    where mc.claim_type = 'institutional' and mc.diagnosis_code_2 is not null
),
inst_dx3 as (
    select
        mc.data_source, mc.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_3' as metric_id,
        'institutional' as claim_scope,
        case when term.icd_10_cm is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.icd_10_cm is not null then concat(mc.diagnosis_code_3,'|',coalesce(term.short_description,'')) else concat(mc.diagnosis_code_3,'|') end as field_value
    from {{ ref('medical_claim') }} mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_3 = term.icd_10_cm
    where mc.claim_type = 'institutional' and mc.diagnosis_code_3 is not null
),

-- Admission and Discharge Codes (Institutional)
inst_admit_source as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:ADMIT_SOURCE_CODE' as metric_id,
        'institutional' as claim_scope,
        case when term.admit_source_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.admit_source_code is not null then concat(b.admit_source_code,'|',coalesce(term.admit_source_description,'')) else concat(b.admit_source_code,'|') end as field_value
    from {{ ref('medical_claim') }} b
    left join {{ ref('terminology__admit_source') }} term on b.admit_source_code = term.admit_source_code
    where b.claim_type = 'institutional' and b.admit_source_code is not null
),
inst_admit_type as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:ADMIT_TYPE_CODE' as metric_id,
        'institutional' as claim_scope,
        case when term.admit_type_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.admit_type_code is not null then concat(b.admit_type_code,'|',coalesce(term.admit_type_description,'')) else concat(b.admit_type_code,'|') end as field_value
    from {{ ref('medical_claim') }} b
    left join {{ ref('terminology__admit_type') }} term on b.admit_type_code = term.admit_type_code
    where b.claim_type = 'institutional' and b.admit_type_code is not null
),
inst_discharge_disposition as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DISCHARGE_DISPOSITION_CODE' as metric_id,
        'institutional' as claim_scope,
        case when term.discharge_disposition_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.discharge_disposition_code is not null then concat(b.discharge_disposition_code,'|',coalesce(term.discharge_disposition_description,'')) else concat(b.discharge_disposition_code,'|') end as field_value
    from {{ ref('medical_claim') }} b
    left join {{ ref('terminology__discharge_disposition') }} term on b.discharge_disposition_code = term.discharge_disposition_code
    where b.claim_type = 'institutional' and b.discharge_disposition_code is not null
),

-- Pharmacy NDC Codes
pharm_ndc as (
    select
        p.data_source, p.payer, {{ quote_column('plan') }} as plan,
        'claims:pharmacy:NDC_CODE' as metric_id,
        'pharmacy' as claim_scope,
        case when term.ndc is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.ndc is not null then concat(p.ndc_code,'|',coalesce(term.rxnorm_description, term.fda_description,'')) else concat(p.ndc_code,'|') end as field_value
    from {{ ref('pharmacy_claim') }} p
    left join {{ ref('terminology__ndc') }} term on p.ndc_code = term.ndc
    where p.ndc_code is not null
),

-- Eligibility terminology checks
elig_gender as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:GENDER' as metric_id,
        'eligibility' as claim_scope,
        case when term.gender is not null then 'valid' else 'invalid' end as bucket_name,
        concat(e.gender,'|') as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__gender') }} term on e.gender = term.gender
    where e.gender is not null
),
elig_race as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:RACE' as metric_id,
        'eligibility' as claim_scope,
        case when term.description is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.description is not null then concat(term.code,'|',coalesce(term.description,'')) else concat(e.race,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__race') }} term on e.race = term.description
    where e.race is not null
),
elig_payer_type as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:PAYER_TYPE' as metric_id,
        'eligibility' as claim_scope,
        case when term.payer_type is not null then 'valid' else 'invalid' end as bucket_name,
        concat(e.payer_type,'|') as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__payer_type') }} term on e.payer_type = term.payer_type
    where e.payer_type is not null
),
elig_medicare_status as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:MEDICARE_STATUS_CODE' as metric_id,
        'eligibility' as claim_scope,
        case when term.medicare_status_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.medicare_status_code is not null then concat(e.medicare_status_code,'|',coalesce(term.medicare_status_description,'')) else concat(e.medicare_status_code,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_status') }} term on e.medicare_status_code = term.medicare_status_code
    where e.medicare_status_code is not null
),
elig_dual_status as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:DUAL_STATUS_CODE' as metric_id,
        'eligibility' as claim_scope,
        case when term.dual_status_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.dual_status_code is not null then concat(e.dual_status_code,'|',coalesce(term.dual_status_description,'')) else concat(e.dual_status_code,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_dual_eligibility') }} term on e.dual_status_code = term.dual_status_code
    where e.dual_status_code is not null
),
elig_orec as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:ORIGINAL_REASON_ENTITLEMENT_CODE' as metric_id,
        'eligibility' as claim_scope,
        case when term.original_reason_entitlement_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.original_reason_entitlement_code is not null then concat(e.original_reason_entitlement_code,'|',coalesce(term.original_reason_entitlement_description,'')) else concat(e.original_reason_entitlement_code,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_orec') }} term on e.original_reason_entitlement_code = term.original_reason_entitlement_code
    where e.original_reason_entitlement_code is not null
),

unioned as (
    select * from drg
    union all select * from bill_type
    union all select * from revenue_center
    union all select * from hcpcs_professional
    union all select * from hcpcs_inst_out
    union all select * from pos_professional
    union all select * from claim_type
    union all select * from proc1
    union all select * from proc2
    union all select * from proc3
    union all select * from prof_dx1
    union all select * from prof_dx2
    union all select * from prof_dx3
    union all select * from inst_dx1
    union all select * from inst_dx2
    union all select * from inst_dx3
    union all select * from inst_admit_source
    union all select * from inst_admit_type
    union all select * from inst_discharge_disposition
    union all select * from pharm_ndc
    union all select * from elig_gender
    union all select * from elig_race
    union all select * from elig_payer_type
    union all select * from elig_medicare_status
    union all select * from elig_dual_status
    union all select * from elig_orec
    -- DRG Code Type (Institutional)
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DRG_CODE_TYPE' as metric_id,
        'institutional' as claim_scope,
        case when term.code_type is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.drg_code_type,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('reference_data__code_type') }} term on m.drg_code_type = term.code_type
    where m.claim_type = 'institutional' and m.drg_code_type is not null
    -- Diagnosis Code Type (Professional/Institutional)
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:DIAGNOSIS_CODE_TYPE' as metric_id,
        'professional' as claim_scope,
        case when term.code_type is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.diagnosis_code_type,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('reference_data__code_type') }} term on m.diagnosis_code_type = term.code_type
    where m.claim_type = 'professional' and m.diagnosis_code_type is not null
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:DIAGNOSIS_CODE_TYPE' as metric_id,
        'institutional' as claim_scope,
        case when term.code_type is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.diagnosis_code_type,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('reference_data__code_type') }} term on m.diagnosis_code_type = term.code_type
    where m.claim_type = 'institutional' and m.diagnosis_code_type is not null
    -- NPIs (Professional)
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:BILLING_NPI' as metric_id,
        'professional' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.billing_npi,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__provider') }} prov on m.billing_npi = prov.npi
    where m.claim_type = 'professional' and m.billing_npi is not null
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:RENDERING_NPI' as metric_id,
        'professional' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.rendering_npi,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__provider') }} prov on m.rendering_npi = prov.npi
    where m.claim_type = 'professional' and m.rendering_npi is not null
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:professional:FACILITY_NPI' as metric_id,
        'professional' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.facility_npi,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__provider') }} prov on m.facility_npi = prov.npi
    where m.claim_type = 'professional' and m.facility_npi is not null
    -- NPIs (Institutional)
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:BILLING_NPI' as metric_id,
        'institutional' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.billing_npi,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__provider') }} prov on m.billing_npi = prov.npi
    where m.claim_type = 'institutional' and m.billing_npi is not null
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:RENDERING_NPI' as metric_id,
        'institutional' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.rendering_npi,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__provider') }} prov on m.rendering_npi = prov.npi
    where m.claim_type = 'institutional' and m.rendering_npi is not null
    union all
    select
        m.data_source, m.payer, {{ quote_column('plan') }} as plan,
        'claims:institutional:FACILITY_NPI' as metric_id,
        'institutional' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(m.facility_npi,'|') as field_value
    from {{ ref('medical_claim') }} m
    left join {{ ref('terminology__provider') }} prov on m.facility_npi = prov.npi
    where m.claim_type = 'institutional' and m.facility_npi is not null
    -- NPIs (Pharmacy)
    union all
    select
        p.data_source, p.payer, {{ quote_column('plan') }} as plan,
        'claims:pharmacy:DISPENSING_PROVIDER_NPI' as metric_id,
        'pharmacy' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(p.dispensing_provider_npi,'|') as field_value
    from {{ ref('pharmacy_claim') }} p
    left join {{ ref('terminology__provider') }} prov on p.dispensing_provider_npi = prov.npi
    where p.dispensing_provider_npi is not null
    union all
    select
        p.data_source, p.payer, {{ quote_column('plan') }} as plan,
        'claims:pharmacy:PRESCRIBING_PROVIDER_NPI' as metric_id,
        'pharmacy' as claim_scope,
        case when prov.npi is not null then 'valid' else 'invalid' end as bucket_name,
        concat(p.prescribing_provider_npi,'|') as field_value
    from {{ ref('pharmacy_claim') }} p
    left join {{ ref('terminology__provider') }} prov on p.prescribing_provider_npi = prov.npi
    where p.prescribing_provider_npi is not null
)
select data_source, payer, {{ quote_column('plan') }} as plan, metric_id, claim_scope, bucket_name, field_value, count(*) as frequency
from unioned
group by data_source, payer, {{ quote_column('plan') }}, metric_id, claim_scope, bucket_name, field_value
