{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
Steps for staging condition data:
    1) Filter to risk-adjustable claims per claim type for the collection year.
    2) Gather diagnosis codes from condition for the eligible claims.
    3) Map and filter diagnosis codes to HCCs

Claims filtering logic:
 - Professional:
    - CPT/HCPCS in CPT/HCPCS seed file from CMS
 - Inpatient:
    - Bill type code in (11X, 41X)
 - Outpatient:
    - Bill type code in (12X, 13X, 43X, 71X, 73X, 76X, 77X, 85X)
    - CPT/HCPCS in CPT/HCPCS seed file from CMS

Jinja is used to set payment year variable.
 - The payment_year var has been set here so it gets compiled.
 - The collection year is one year prior to the payment year.
*/

{% set payment_year = var('cms_hcc_payment_year') | int() -%}
{% set collection_year = payment_year - 1 -%}

with medical_claims as (

    select
          claim_id
        , claim_line_number
        , claim_type
        , patient_id
        , claim_start_date
        , claim_end_date
        , bill_type_code
        , hcpcs_code
    from {{ ref('cms_hcc__stg_core__medical_claim') }}

)

, conditions as (

    select
          claim_id
        , patient_id
        , code
    from {{ ref('cms_hcc__stg_core__condition') }}
    where code_type = 'icd-10-cm'

)

, cpt_hcpcs_list as (

    select
          payment_year
        , hcpcs_cpt_code
    from {{ ref('cms_hcc__cpt_hcpcs') }}

)

, professional_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.patient_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
    where claim_type = 'professional'
        and extract(year from claim_end_date) = {{ collection_year }}
        and cpt_hcpcs_list.payment_year = {{ payment_year }}

)

, inpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.patient_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
    from medical_claims
    where claim_type = 'institutional'
        and extract(year from claim_end_date) = {{ collection_year }}
        and left(bill_type_code,2) in ('11','41')

)

, outpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.patient_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
    where claim_type = 'institutional'
        and extract(year from claim_end_date) = {{ collection_year }}
        and cpt_hcpcs_list.payment_year = {{ payment_year }}
        and left(bill_type_code,2) in ('12','13','43','71','73','76','77','85')

)

, eligible_claims as (

    select * from professional_claims
    union all
    select * from inpatient_claims
    union all
    select * from outpatient_claims

)

, eligible_conditions as (

    select distinct
          eligible_claims.claim_id
        , eligible_claims.patient_id
        , conditions.code
    from eligible_claims
        inner join conditions
            on eligible_claims.claim_id = conditions.claim_id
            and eligible_claims.patient_id = conditions.patient_id

)

, add_data_types as (

    select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(code as {{ dbt.type_string() }}) as condition_code
        , cast('{{ payment_year }}' as integer) as payment_year
    from eligible_conditions

)

select
      patient_id
    , condition_code
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types