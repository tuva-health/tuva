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
    select
        m.data_source,
        'claims:institutional_inpatient:DRG_CODE' as metric_id,
        'DRG Code (Inpatient)'                  as metric_name,
        'institutional_inpatient'               as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'Institutional inpatient claims'                           as denominator_desc
    from {{ ref('data_quality__institutional_drg_code') }} as m
    group by m.data_source
),

bill_type as (
    select
        m.data_source,
        'claims:institutional:BILL_TYPE_CODE'   as metric_id,
        'Bill Type Code (Institutional)'        as metric_name,
        'institutional'                         as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from {{ ref('data_quality__institutional_bill_type_code') }} as m
    group by m.data_source
),

revenue_center as (
    select
        m.data_source,
        'claims:institutional:REVENUE_CENTER_CODE' as metric_id,
        'Revenue Center Code (Institutional)'      as metric_name,
        'institutional'                             as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'Institutional claim lines'                                as denominator_desc
    from {{ ref('data_quality__institutional_revenue_center_code') }} as m
    group by m.data_source
),

-- HCPCS (Professional): mirror data_quality__claim_hcpcs_code but restrict to professional claim_type for denominator scope
hcpcs_professional as (
    select
        mc.data_source,
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
    group by mc.data_source
),

-- HCPCS (Institutional Outpatient): institutional claim_type excluding inpatient (bill_type prefix '11')
hcpcs_institutional_outpatient as (
    select
        mc.data_source,
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
    group by mc.data_source
),

-- Claim Type (All Medical Claims)
claim_type as (
    select
        m.data_source,
        'claims:medical:CLAIM_TYPE'            as metric_id,
        'Claim Type (Medical)'                 as metric_name,
        'medical'                              as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'All medical claims'                                       as denominator_desc
    from {{ ref('data_quality__claim_claim_type') }} as m
    group by m.data_source
),

-- Professional POS
pos_professional as (
    select
        m.data_source,
        'claims:professional:PLACE_OF_SERVICE_CODE' as metric_id,
        'Place of Service (Professional)'           as metric_name,
        'professional'                               as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        0                                                          as multiple_n,
        count(*)                                                   as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('data_quality__professional_place_of_service_code') }} as m
    group by m.data_source
),

-- Professional NPIs
prof_billing_npi as (
    select
        m.data_source,
        'claims:professional:BILLING_NPI'      as metric_id,
        'Billing NPI (Professional)'           as metric_name,
        'professional'                         as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        0                                                          as multiple_n,
        count(*)                                                   as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('data_quality__professional_billing_npi') }} as m
    group by m.data_source
),

prof_rendering_npi as (
    select
        m.data_source,
        'claims:professional:RENDERING_NPI'     as metric_id,
        'Rendering NPI (Professional)'          as metric_name,
        'professional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        0                                                          as multiple_n,
        count(*)                                                   as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('data_quality__professional_rendering_npi') }} as m
    group by m.data_source
),

prof_facility_npi as (
    select
        m.data_source,
        'claims:professional:FACILITY_NPI'      as metric_id,
        'Facility NPI (Professional)'           as metric_name,
        'professional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        0                                                          as multiple_n,
        count(*)                                                   as denominator_n,
        'Professional claim lines'                                 as denominator_desc
    from {{ ref('data_quality__professional_facility_npi') }} as m
    group by m.data_source
),

-- Institutional NPIs (claim-level, includes multiple detection)
inst_billing_npi as (
    select
        m.data_source,
        'claims:institutional:BILLING_NPI'      as metric_id,
        'Billing NPI (Institutional)'           as metric_name,
        'institutional'                         as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from {{ ref('data_quality__institutional_billing_npi') }} as m
    group by m.data_source
),

inst_rendering_npi as (
    select
        m.data_source,
        'claims:institutional:RENDERING_NPI'     as metric_id,
        'Rendering NPI (Institutional)'          as metric_name,
        'institutional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from {{ ref('data_quality__institutional_rendering_npi') }} as m
    group by m.data_source
),

inst_facility_npi as (
    select
        m.data_source,
        'claims:institutional:FACILITY_NPI'      as metric_id,
        'Facility NPI (Institutional)'           as metric_name,
        'institutional'                          as claim_scope,
        sum(case when bucket_name = 'valid' then 1 else 0 end)     as valid_n,
        sum(case when bucket_name = 'invalid' then 1 else 0 end)   as invalid_n,
        sum(case when bucket_name = 'null' then 1 else 0 end)      as null_n,
        sum(case when bucket_name = 'multiple' then 1 else 0 end)  as multiple_n,
        count(*)                                                   as denominator_n,
        'Institutional claims'                                     as denominator_desc
    from {{ ref('data_quality__institutional_facility_npi') }} as m
    group by m.data_source
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
    select * from prof_dx1
    union all
    select * from prof_dx2
    union all
    select * from prof_dx3
    union all
    select * from inst_dx1
    union all
    select * from inst_dx2
    union all
    select * from inst_dx3
    union all
    select * from inst_proc1
    union all
    select * from inst_proc2
    union all
    select * from inst_proc3
    union all
    select * from inst_admit_source
    union all
    select * from inst_admit_type
    union all
    select * from inst_discharge_disposition
    union all
    select * from pharm_ndc
    union all
    select * from pharm_dispensing_npi
    union all
    select * from pharm_prescribing_npi
    union all
    select * from elig_gender
    union all
    select * from elig_race
    union all
    select * from elig_payer_type
    union all
    select * from elig_medicare_status
    union all
    select * from elig_dual_status
    union all
    select * from elig_orec
)

select
    data_source,
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
 
