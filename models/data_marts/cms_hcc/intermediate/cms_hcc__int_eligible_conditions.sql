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
          claim_id
        , claim_line_number
        , claim_type
        , payer
        , person_id
        , rendering_id as rendering_npi
        , claim_start_date
        , claim_end_date
        , bill_type_code
        , hcpcs_code
    from {{ ref('cms_hcc__stg_core__medical_claim') }}

)

, conditions as (

    select
          claim_id
        , payer
        , person_id
        , code
    from {{ ref('cms_hcc__stg_core__condition') }}
    where code_type = 'icd-10-cm'

)

, cpt_hcpcs_list as (

    select
        -- This is mislabelled as payment year, it should be collection year
        -- TODO: Update the label in the seed file
          payment_year as collection_year
        , hcpcs_cpt_code
    from {{ ref('cms_hcc__cpt_hcpcs') }}

    union all
    
    -- Adding a mapping for the next year copying the current year mappings
    select
          payment_year + 1 as collection_year
        , hcpcs_cpt_code
    from {{ ref('cms_hcc__cpt_hcpcs') }}
    where payment_year = (select max(payment_year) as payment_year from {{ ref('cms_hcc__cpt_hcpcs') }})

)

/*
    Aggregate monthly collection dates to yearly boundaries to avoid
    expanding claims by up to 12x at this early stage. The monthly
    grain is deferred to after the condition join where the dataset
    is much smaller.
*/
, yearly_collection_dates as (

    select
          payment_year
        , min(collection_start_date) as collection_start_date
        , max(collection_end_date) as collection_end_date
    from {{ ref('cms_hcc__int_monthly_collection_dates') }}
    group by payment_year

)

, professional_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.payer
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , dates.payment_year
        , coalesce(medical_claims.claim_end_date, medical_claims.claim_start_date) as condition_date
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
        inner join yearly_collection_dates as dates
            on coalesce(claim_end_date, claim_start_date) between dates.collection_start_date and dates.collection_end_date
            and cpt_hcpcs_list.collection_year + 1 = dates.payment_year
    where claim_type = 'professional'
)

, inpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.payer
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , dates.payment_year
        , coalesce(medical_claims.claim_end_date, medical_claims.claim_start_date) as condition_date
    from medical_claims
        inner join yearly_collection_dates as dates
            on coalesce(claim_end_date, claim_start_date) between dates.collection_start_date and dates.collection_end_date
    where claim_type = 'institutional'
        and substring(bill_type_code, 1, 2) in ('11', '41')

)

, outpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.payer
        , medical_claims.person_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
        , dates.payment_year
        , coalesce(medical_claims.claim_end_date, medical_claims.claim_start_date) as condition_date
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
        inner join yearly_collection_dates as dates
            on coalesce(claim_end_date, claim_start_date) between dates.collection_start_date and dates.collection_end_date
            and cpt_hcpcs_list.collection_year + 1 = dates.payment_year
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
        , eligible_claims.claim_line_number
        , eligible_claims.payer
        , eligible_claims.person_id
        , eligible_claims.payment_year
        , eligible_claims.condition_date
        , conditions.code
    from eligible_claims
        inner join conditions
            on eligible_claims.claim_id = conditions.claim_id
            and eligible_claims.person_id = conditions.person_id
            and eligible_claims.payer = conditions.payer

)

/*
    Expand to monthly grain after condition deduplication. Each condition
    appears in all collection months where its claim date falls within
    the cumulative collection window (condition_date <= collection_end_date).
*/
, eligible_conditions_monthly as (

    select distinct
          ec.claim_id
        , ec.claim_line_number
        , ec.payer
        , ec.person_id
        , ec.payment_year
        , dates.collection_start_date
        , dates.collection_end_date
        , ec.code
    from eligible_conditions as ec
        inner join {{ ref('cms_hcc__int_monthly_collection_dates') }} as dates
            on ec.payment_year = dates.payment_year
            and ec.condition_date <= dates.collection_end_date

)

, add_data_types as (

    select distinct
          cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(claim_line_number as {{ dbt.type_string() }}) as claim_line_number
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(code as {{ dbt.type_string() }}) as condition_code
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from eligible_conditions_monthly

)

select
      person_id
    , claim_id
    , claim_line_number
    , payer
    , condition_code
    , payment_year
    , collection_start_date
    , collection_end_date
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from add_data_types
