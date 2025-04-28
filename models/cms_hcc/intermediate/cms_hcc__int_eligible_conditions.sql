{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
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

with medical_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.admission_date
        , medical_claims.discharge_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , dates.payment_year
        , dates.collection_start_date
        , dates.collection_end_date  
    from {{ ref('cms_hcc__stg_core__medical_claim') }} as medical_claims
    inner join {{ ref('cms_hcc__int_monthly_collection_dates') }} as dates
        on medical_claims.claim_end_date between dates.collection_start_date and dates.collection_end_date
)

, conditions as (

    select
          claim_id
        , person_id
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
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.admission_date
        , medical_claims.discharge_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , medical_claims.payment_year
        , medical_claims.collection_start_date
        , medical_claims.collection_end_date
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
            and cpt_hcpcs_list.payment_year = medical_claims.payment_year
    where claim_type = 'professional'

)

, inpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.admission_date
        , medical_claims.discharge_date        
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , dates.payment_year
        , dates.collection_start_date
        , dates.collection_end_date
    from medical_claims
    left join {{ ref('cms_hcc__int_covid_episodes') }} as covid
        on  medical_claims.person_id = covid.person_id
        -- NOTE: Assuming if a claim starts within the COVID-19 window, then it is excluded despite possibly continuing beyond
        -- the COVID-19 episode window
        and date_trunc('month', medical_claims.admission_date) = covid.yearmo_trunc
    where claim_type = 'institutional'
        and substring(bill_type_code, 1, 2) in ('11', '41')
        and covid.person_id is null -- Exclude any covid episodes for inpatient claims

)

, outpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.admission_date
        , medical_claims.discharge_date        
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , medical_claims.payment_year
        , medical_claims.collection_start_date
        , medical_claims.collection_end_date
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
            and cpt_hcpcs_list.payment_year = medical_claims.payment_year
    where claim_type = 'institutional'
        and substring(bill_type_code, 1, 2) in ('12', '13', '43', '71', '73', '76', '77', '85')

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
        , eligible_claims.person_id
        , eligible_claims.payment_year
        , eligible_claims.collection_start_date
        , eligible_claims.collection_end_date
        , eligible_claims.admission_date
        , eligible_claims.discharge_date
        , conditions.code
    from eligible_claims
        inner join conditions
            on eligible_claims.claim_id = conditions.claim_id
            and eligible_claims.person_id = conditions.person_id

)

, add_data_types as (

    select distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(code as {{ dbt.type_string() }}) as condition_code
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from eligible_conditions

)

select
      person_id
    , condition_code
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
