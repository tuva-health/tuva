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

Note: Null/multiple buckets are handled in metrics, not here. Uses centralized
terminology checks to determine validity and joins to source tables for codes.
*/

with inst_inpt as (
    select * from {{ ref('data_quality__claims_institutional_inpatient_checks') }}
), inst as (
    select * from {{ ref('data_quality__claims_institutional_checks') }}
), prof as (
    select * from {{ ref('data_quality__claims_professional_checks') }}
), pharm as (
    select * from {{ ref('data_quality__pharmacy_checks') }}
)

-- Institutional inpatient
, drg as (
    select
        pc.data_source,
        pc.payer,
        pc.plan as {{ quote_column('plan') }},
        'claims:institutional_inpatient:DRG_CODE' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case
            when pc.has_valid = 1 and b.drg_code_type = 'ms-drg' and ms.ms_drg_code is not null
                then concat(b.drg_code,'|',coalesce(ms.ms_drg_description,''))
            when pc.has_valid = 1 and b.drg_code_type = 'apr-drg' and apr.apr_drg_code is not null
                then concat(b.drg_code,'|',coalesce(apr.apr_drg_description,''))
            else concat(b.drg_code,'|')
        end as field_value
    from inst_inpt pc
    join {{ ref('medical_claim') }} b on b.claim_id = pc.claim_id and b.data_source = pc.data_source and b.claim_type = 'institutional'
    left join {{ ref('terminology__ms_drg') }} ms on b.drg_code_type='ms-drg' and b.drg_code = ms.ms_drg_code
    left join {{ ref('terminology__apr_drg') }} apr on b.drg_code_type='apr-drg' and b.drg_code = apr.apr_drg_code
    where (pc.has_valid = 1 or pc.has_invalid = 1) and b.drg_code is not null
)
, proc1 as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        'claims:institutional_inpatient:PROCEDURE_CODE_1' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case when pc.has_valid = 1 and term.icd_10_pcs is not null then concat(b.procedure_code_1,'|',coalesce(term.description,'')) else concat(b.procedure_code_1,'|') end as field_value
    from inst_inpt pc
    join {{ ref('medical_claim') }} b on b.claim_id = pc.claim_id and b.data_source = pc.data_source and b.claim_type='institutional'
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_1 = term.icd_10_pcs
    where pc.metric_id = 'claims:institutional_inpatient:PROCEDURE_CODE_1' and (pc.has_valid = 1 or pc.has_invalid = 1) and b.procedure_code_1 is not null
)
, proc2 as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        'claims:institutional_inpatient:PROCEDURE_CODE_2' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case when pc.has_valid = 1 and term.icd_10_pcs is not null then concat(b.procedure_code_2,'|',coalesce(term.description,'')) else concat(b.procedure_code_2,'|') end as field_value
    from inst_inpt pc
    join {{ ref('medical_claim') }} b on b.claim_id = pc.claim_id and b.data_source = pc.data_source and b.claim_type='institutional'
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_2 = term.icd_10_pcs
    where pc.metric_id = 'claims:institutional_inpatient:PROCEDURE_CODE_2' and (pc.has_valid = 1 or pc.has_invalid = 1) and b.procedure_code_2 is not null
)
, proc3 as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        'claims:institutional_inpatient:PROCEDURE_CODE_3' as metric_id,
        'institutional_inpatient' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case when pc.has_valid = 1 and term.icd_10_pcs is not null then concat(b.procedure_code_3,'|',coalesce(term.description,'')) else concat(b.procedure_code_3,'|') end as field_value
    from inst_inpt pc
    join {{ ref('medical_claim') }} b on b.claim_id = pc.claim_id and b.data_source = pc.data_source and b.claim_type='institutional'
    left join {{ ref('terminology__icd_10_pcs') }} term on b.procedure_code_3 = term.icd_10_pcs
    where pc.metric_id = 'claims:institutional_inpatient:PROCEDURE_CODE_3' and (pc.has_valid = 1 or pc.has_invalid = 1) and b.procedure_code_3 is not null
)

-- Institutional non-inpatient (claim-level)
, inst_claim_level as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        pc.metric_id,
        'institutional' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case pc.metric_id
            when 'claims:institutional:BILL_TYPE_CODE' then case when pc.has_valid=1 then concat(b.bill_type_code,'|',coalesce(bt.bill_type_description,'')) else concat(b.bill_type_code,'|') end
            when 'claims:institutional:DIAGNOSIS_CODE_1' then case when pc.has_valid=1 then concat(b.diagnosis_code_1,'|',coalesce(dx1.short_description,'')) else concat(b.diagnosis_code_1,'|') end
            when 'claims:institutional:DIAGNOSIS_CODE_2' then case when pc.has_valid=1 then concat(b.diagnosis_code_2,'|',coalesce(dx2.short_description,'')) else concat(b.diagnosis_code_2,'|') end
            when 'claims:institutional:DIAGNOSIS_CODE_3' then case when pc.has_valid=1 then concat(b.diagnosis_code_3,'|',coalesce(dx3.short_description,'')) else concat(b.diagnosis_code_3,'|') end
            when 'claims:institutional:ADMIT_SOURCE_CODE' then case when pc.has_valid=1 then concat(b.admit_source_code,'|',coalesce(asrc.admit_source_description,'')) else concat(b.admit_source_code,'|') end
            when 'claims:institutional:ADMIT_TYPE_CODE' then case when pc.has_valid=1 then concat(b.admit_type_code,'|',coalesce(atyp.admit_type_description,'')) else concat(b.admit_type_code,'|') end
            when 'claims:institutional:DISCHARGE_DISPOSITION_CODE' then case when pc.has_valid=1 then concat(b.discharge_disposition_code,'|',coalesce(dd.discharge_disposition_description,'')) else concat(b.discharge_disposition_code,'|') end
            when 'claims:institutional:BILLING_NPI' then concat(b.billing_npi,'|')
            when 'claims:institutional:RENDERING_NPI' then concat(b.rendering_npi,'|')
            when 'claims:institutional:FACILITY_NPI' then concat(b.facility_npi,'|')
        end as field_value
    from inst pc
    join {{ ref('medical_claim') }} b on b.claim_id = pc.claim_id and b.data_source = pc.data_source and b.claim_type='institutional'
    left join {{ ref('terminology__bill_type') }} bt on pc.metric_id='claims:institutional:BILL_TYPE_CODE' and b.bill_type_code = bt.bill_type_code
    left join {{ ref('terminology__icd_10_cm') }} dx1 on pc.metric_id='claims:institutional:DIAGNOSIS_CODE_1' and b.diagnosis_code_1 = dx1.icd_10_cm
    left join {{ ref('terminology__icd_10_cm') }} dx2 on pc.metric_id='claims:institutional:DIAGNOSIS_CODE_2' and b.diagnosis_code_2 = dx2.icd_10_cm
    left join {{ ref('terminology__icd_10_cm') }} dx3 on pc.metric_id='claims:institutional:DIAGNOSIS_CODE_3' and b.diagnosis_code_3 = dx3.icd_10_cm
    left join {{ ref('terminology__admit_source') }} asrc on pc.metric_id='claims:institutional:ADMIT_SOURCE_CODE' and b.admit_source_code = asrc.admit_source_code
    left join {{ ref('terminology__admit_type') }} atyp on pc.metric_id='claims:institutional:ADMIT_TYPE_CODE' and b.admit_type_code = atyp.admit_type_code
    left join {{ ref('terminology__discharge_disposition') }} dd on pc.metric_id='claims:institutional:DISCHARGE_DISPOSITION_CODE' and b.discharge_disposition_code = dd.discharge_disposition_code
    where pc.metric_id in (
        'claims:institutional:BILL_TYPE_CODE',
        'claims:institutional:BILLING_NPI',
        'claims:institutional:RENDERING_NPI',
        'claims:institutional:FACILITY_NPI',
        'claims:institutional:DIAGNOSIS_CODE_1',
        'claims:institutional:DIAGNOSIS_CODE_2',
        'claims:institutional:DIAGNOSIS_CODE_3',
        'claims:institutional:ADMIT_SOURCE_CODE',
        'claims:institutional:ADMIT_TYPE_CODE',
        'claims:institutional:DISCHARGE_DISPOSITION_CODE'
    ) and (pc.has_valid = 1 or pc.has_invalid = 1)
)

-- Institutional line-level
, inst_line_level as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        pc.metric_id,
        case pc.metric_id when 'claims:institutional_outpatient:HCPCS_CODE' then 'institutional_outpatient' else 'institutional' end as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case pc.metric_id
            when 'claims:institutional:REVENUE_CENTER_CODE' then case when pc.has_valid=1 then concat(b.revenue_center_code,'|',coalesce(rv.revenue_center_description,'')) else concat(b.revenue_center_code,'|') end
            when 'claims:institutional_outpatient:HCPCS_CODE' then case when pc.has_valid=1 then concat(b.hcpcs_code,'|',coalesce(h2.short_description,'')) else concat(b.hcpcs_code,'|') end
        end as field_value
    from inst pc
    join {{ ref('medical_claim') }} b
      on b.claim_id = pc.claim_id and b.claim_line_number = pc.claim_line_number and b.data_source = pc.data_source and b.claim_type='institutional'
    left join {{ ref('terminology__revenue_center') }} rv on pc.metric_id='claims:institutional:REVENUE_CENTER_CODE' and b.revenue_center_code = rv.revenue_center_code
    left join {{ ref('terminology__hcpcs_level_2') }} h2 on pc.metric_id='claims:institutional_outpatient:HCPCS_CODE' and b.hcpcs_code = h2.hcpcs
    where pc.metric_id in (
        'claims:institutional:REVENUE_CENTER_CODE',
        'claims:institutional_outpatient:HCPCS_CODE'
    ) and (pc.has_valid = 1 or pc.has_invalid = 1)
)

-- Professional line-level
, prof_values as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        pc.metric_id,
        'professional' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case pc.metric_id
            when 'claims:professional:HCPCS_CODE' then case when pc.has_valid=1 then concat(b.hcpcs_code,'|',coalesce(h2.short_description,'')) else concat(b.hcpcs_code,'|') end
            when 'claims:professional:PLACE_OF_SERVICE_CODE' then case when pc.has_valid=1 then concat(b.place_of_service_code,'|',coalesce(pos.place_of_service_description,'')) else concat(b.place_of_service_code,'|') end
            when 'claims:professional:DIAGNOSIS_CODE_1' then case when pc.has_valid=1 then concat(b.diagnosis_code_1,'|',coalesce(dx1.short_description,'')) else concat(b.diagnosis_code_1,'|') end
            when 'claims:professional:DIAGNOSIS_CODE_2' then case when pc.has_valid=1 then concat(b.diagnosis_code_2,'|',coalesce(dx2.short_description,'')) else concat(b.diagnosis_code_2,'|') end
            when 'claims:professional:DIAGNOSIS_CODE_3' then case when pc.has_valid=1 then concat(b.diagnosis_code_3,'|',coalesce(dx3.short_description,'')) else concat(b.diagnosis_code_3,'|') end
            when 'claims:professional:BILLING_NPI' then concat(b.billing_npi,'|')
            when 'claims:professional:RENDERING_NPI' then concat(b.rendering_npi,'|')
            when 'claims:professional:FACILITY_NPI' then concat(b.facility_npi,'|')
        end as field_value
    from prof pc
    join {{ ref('medical_claim') }} b
      on b.claim_id = pc.claim_id and b.claim_line_number = pc.claim_line_number and b.data_source = pc.data_source and b.claim_type='professional'
    left join {{ ref('terminology__hcpcs_level_2') }} h2 on pc.metric_id='claims:professional:HCPCS_CODE' and b.hcpcs_code = h2.hcpcs
    left join {{ ref('terminology__place_of_service') }} pos on pc.metric_id='claims:professional:PLACE_OF_SERVICE_CODE' and b.place_of_service_code = pos.place_of_service_code
    left join {{ ref('terminology__icd_10_cm') }} dx1 on pc.metric_id='claims:professional:DIAGNOSIS_CODE_1' and b.diagnosis_code_1 = dx1.icd_10_cm
    left join {{ ref('terminology__icd_10_cm') }} dx2 on pc.metric_id='claims:professional:DIAGNOSIS_CODE_2' and b.diagnosis_code_2 = dx2.icd_10_cm
    left join {{ ref('terminology__icd_10_cm') }} dx3 on pc.metric_id='claims:professional:DIAGNOSIS_CODE_3' and b.diagnosis_code_3 = dx3.icd_10_cm
    where (pc.has_valid = 1 or pc.has_invalid = 1)
)

-- Pharmacy line-level
, pharm_values as (
    select
        pc.data_source, pc.payer, pc.plan as {{ quote_column('plan') }},
        pc.metric_id,
        'pharmacy' as claim_scope,
        case when pc.has_valid = 1 then 'valid' when pc.has_invalid = 1 then 'invalid' end as bucket_name,
        case pc.metric_id
            when 'claims:pharmacy:NDC_CODE' then case when pc.has_valid=1 then concat(p.ndc_code,'|',coalesce(ndc.rxnorm_description, ndc.fda_description,'')) else concat(p.ndc_code,'|') end
            when 'claims:pharmacy:DISPENSING_PROVIDER_NPI' then concat(p.dispensing_provider_npi,'|')
            when 'claims:pharmacy:PRESCRIBING_PROVIDER_NPI' then concat(p.prescribing_provider_npi,'|')
        end as field_value
    from pharm pc
    join {{ ref('pharmacy_claim') }} p on p.claim_id = pc.claim_id and p.claim_line_number = pc.claim_line_number and p.data_source = pc.data_source
    left join {{ ref('terminology__ndc') }} ndc on pc.metric_id='claims:pharmacy:NDC_CODE' and p.ndc_code = ndc.ndc
    where (pc.has_valid = 1 or pc.has_invalid = 1)
)

-- Eligibility (directly from table for values)
, elig_gender as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:GENDER' as metric_id,
        'eligibility' as claim_scope,
        case when term.gender is not null then 'valid' else 'invalid' end as bucket_name,
        concat(e.gender,'|') as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__gender') }} term on e.gender = term.gender
    where e.gender is not null
)
, elig_race as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:RACE' as metric_id,
        'eligibility' as claim_scope,
        case when term.description is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.description is not null then concat(term.code,'|',coalesce(term.description,'')) else concat(e.race,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__race') }} term on e.race = term.description
    where e.race is not null
)
, elig_payer_type as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:PAYER_TYPE' as metric_id,
        'eligibility' as claim_scope,
        case when term.payer_type is not null then 'valid' else 'invalid' end as bucket_name,
        concat(e.payer_type,'|') as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__payer_type') }} term on e.payer_type = term.payer_type
    where e.payer_type is not null
)
, elig_medicare_status as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:MEDICARE_STATUS_CODE' as metric_id,
        'eligibility' as claim_scope,
        case when term.medicare_status_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.medicare_status_code is not null then concat(e.medicare_status_code,'|',coalesce(term.medicare_status_description,'')) else concat(e.medicare_status_code,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_status') }} term on e.medicare_status_code = term.medicare_status_code
    where e.medicare_status_code is not null
)
, elig_dual_status as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:DUAL_STATUS_CODE' as metric_id,
        'eligibility' as claim_scope,
        case when term.dual_status_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.dual_status_code is not null then concat(e.dual_status_code,'|',coalesce(term.dual_status_description,'')) else concat(e.dual_status_code,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_dual_eligibility') }} term on e.dual_status_code = term.dual_status_code
    where e.dual_status_code is not null
)
, elig_orec as (
    select
        e.data_source, e.payer, {{ quote_column('plan') }} as plan,
        'claims:eligibility:ORIGINAL_REASON_ENTITLEMENT_CODE' as metric_id,
        'eligibility' as claim_scope,
        case when term.original_reason_entitlement_code is not null then 'valid' else 'invalid' end as bucket_name,
        case when term.original_reason_entitlement_code is not null then concat(e.original_reason_entitlement_code,'|',coalesce(term.original_reason_entitlement_description,'')) else concat(e.original_reason_entitlement_code,'|') end as field_value
    from {{ ref('eligibility') }} e
    left join {{ ref('terminology__medicare_orec') }} term on e.original_reason_entitlement_code = term.original_reason_entitlement_code
    where e.original_reason_entitlement_code is not null
)

select data_source, payer, {{ quote_column('plan') }} as plan, metric_id, claim_scope, bucket_name, field_value, count(*) as frequency
from (
    select * from drg
    union all select * from proc1
    union all select * from proc2
    union all select * from proc3
    union all select * from inst_claim_level
    union all select * from inst_line_level
    union all select * from prof_values
    union all select * from pharm_values
    union all select * from elig_gender
    union all select * from elig_race
    union all select * from elig_payer_type
    union all select * from elig_medicare_status
    union all select * from elig_dual_status
    union all select * from elig_orec
) u
group by data_source, payer, {{ quote_column('plan') }}, metric_id, claim_scope, bucket_name, field_value
