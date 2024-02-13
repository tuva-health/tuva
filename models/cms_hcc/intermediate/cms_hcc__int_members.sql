{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
Steps for transforming eligibility data into member demographics:
    1) Determine enrollment status using eligibility from the collection year.
    2) Roll up to latest eligibility record for enrollment statuses.
    3) Add age groups based on the payment year.
    4) Determine other statuses.

Jinja is used to set payment year variable.
 - The payment_year var has been set here so it gets compiled.
 - CMS guidance: Age is calculated as of Feb 1 of the payment year.
 - The collection year is one year prior to the payment year.
*/

{% set payment_year = var('cms_hcc_payment_year') | int() -%}
{% set payment_year_age_date = payment_year ~ '-02-01' -%}
{% set collection_year = payment_year - 1 -%}
{% set collection_year_start = collection_year ~ '-01-01' -%}
{% set collection_year_end = collection_year ~ '-12-31' -%}

with stg_eligibility as (

    select
          patient_id
        , enrollment_start_date
        , enrollment_end_date
        , original_reason_entitlement_code
        , dual_status_code
        , medicare_status_code
        , row_number() over(
            partition by patient_id
            order by enrollment_end_date desc
        ) as row_num /* used to dedupe eligibility */
    from {{ ref('cms_hcc__stg_core__eligibility') }}
    where (
        /* filter to members with eligibility in collection or payment year */
        extract(year from enrollment_start_date)
            between {{ collection_year }}
            and {{ payment_year }}
        or extract(year from enrollment_end_date)
            between {{ collection_year }}
            and {{ payment_year }}
    )

)

, stg_patient as (

    select
          patient_id
        , sex
        , birth_date
        , floor({{ datediff('birth_date', "'"~payment_year_age_date~"'", 'hour') }} / 8766.0) as payment_year_age
        , death_date
    from {{ ref('cms_hcc__stg_core__patient') }}

)

/* create proxy enrollment dates if outside of the collection year */
, cap_collection_start_end_dates as (

    select
          patient_id
        , enrollment_start_date
        , enrollment_end_date
        , case
            when enrollment_start_date < '{{ collection_year_start }}'
            then '{{ collection_year_start }}'
            else enrollment_start_date
          end as proxy_enrollment_start_date
        , case
            when enrollment_end_date > '{{ collection_year_end }}'
            then '{{ collection_year_end }}'
            else enrollment_end_date
          end as proxy_enrollment_end_date
    from stg_eligibility
    where (
        /* filter to members with eligibility in collection or payment year */
        extract(year from enrollment_start_date)
            between {{ collection_year }}
            and {{ payment_year }}
        or extract(year from enrollment_end_date)
            between {{ collection_year }}
            and {{ payment_year }}
    )

)

, calculate_prior_coverage as (

    select patient_id
        , sum({{ datediff('proxy_enrollment_start_date', 'proxy_enrollment_end_date', 'month') }} + 1) as coverage_months  /* include starting month */
    from cap_collection_start_end_dates
    group by patient_id

)

/*
   CMS guidance: A “New Enrollee” status is when a beneficiary has less than
   12 months of coverage prior to the payment year.
*/
, add_enrollment as (

    select
          patient_id
        , case
            when coverage_months < 12 then 'New'
            else 'Continuing'
          end as enrollment_status
    from calculate_prior_coverage

)

, latest_eligibility as (

    select
          stg_eligibility.patient_id
        , stg_patient.sex as gender
        , stg_patient.payment_year_age
        , stg_eligibility.original_reason_entitlement_code
        , stg_eligibility.dual_status_code
        , stg_eligibility.medicare_status_code
        /* Defaulting to "New" enrollment status when missing */
        , case
            when add_enrollment.enrollment_status is null then 'New'
            else add_enrollment.enrollment_status
          end as enrollment_status
        , case
            when add_enrollment.enrollment_status is null then TRUE
            else FALSE
          end as enrollment_status_default
    from stg_eligibility
        left join add_enrollment
            on stg_eligibility.patient_id = add_enrollment.patient_id
        left join stg_patient
            on stg_eligibility.patient_id = stg_patient.patient_id
    where stg_eligibility.row_num = 1

)

, add_age_group as (

    select
          patient_id
        , gender
        , payment_year_age
        , original_reason_entitlement_code
        , dual_status_code
        , medicare_status_code
        , enrollment_status
        , enrollment_status_default
        , case
            when enrollment_status = 'Continuing' and payment_year_age between 0 and 34 then '0-34'
            when enrollment_status = 'Continuing' and payment_year_age between 35 and 44 then '35-44'
            when enrollment_status = 'Continuing' and payment_year_age between 45 and 54 then '45-54'
            when enrollment_status = 'Continuing' and payment_year_age between 55 and 59 then '55-59'
            when enrollment_status = 'Continuing' and payment_year_age between 60 and 64 then '60-64'
            when enrollment_status = 'Continuing' and payment_year_age between 65 and 69 then '65-69'
            when enrollment_status = 'Continuing' and payment_year_age between 70 and 74 then '70-74'
            when enrollment_status = 'Continuing' and payment_year_age between 75 and 79 then '75-79'
            when enrollment_status = 'Continuing' and payment_year_age between 80 and 84 then '80-84'
            when enrollment_status = 'Continuing' and payment_year_age between 85 and 89 then '85-89'
            when enrollment_status = 'Continuing' and payment_year_age between 90 and 94 then '90-94'
            when enrollment_status = 'Continuing' and payment_year_age >= 95 then '>=95'
            when enrollment_status = 'New' and payment_year_age between 0 and 34 then '0-34'
            when enrollment_status = 'New' and payment_year_age between 35 and 44 then '35-44'
            when enrollment_status = 'New' and payment_year_age between 45 and 54 then '45-54'
            when enrollment_status = 'New' and payment_year_age between 55 and 59 then '55-59'
            when enrollment_status = 'New' and payment_year_age between 60 and 64 then '60-64'
            when enrollment_status = 'New' and payment_year_age = 65 then '65'
            when enrollment_status = 'New' and payment_year_age = 66 then '66'
            when enrollment_status = 'New' and payment_year_age = 67 then '67'
            when enrollment_status = 'New' and payment_year_age = 68 then '68'
            when enrollment_status = 'New' and payment_year_age = 69 then '69'
            when enrollment_status = 'New' and payment_year_age between 70 and 74 then '70-74'
            when enrollment_status = 'New' and payment_year_age between 75 and 79 then '75-79'
            when enrollment_status = 'New' and payment_year_age between 80 and 84 then '80-84'
            when enrollment_status = 'New' and payment_year_age between 85 and 89 then '85-89'
            when enrollment_status = 'New' and payment_year_age between 90 and 94 then '90-94'
            when enrollment_status = 'New' and payment_year_age >= 95 then '>=95'
          end as age_group
    from latest_eligibility

)

, add_status_logic as (

    select
          patient_id
        , enrollment_status
        , case
            when gender = 'female' then 'Female'
            when gender = 'male' then 'Male'
            else null
          end as gender
        , age_group
        , case
            when dual_status_code in ('01','02','03','04','05','06','08') then 'Yes'
            else 'No'
          end as medicaid_status
        , case
            when dual_status_code in ('02','04','08') then 'Full'
            when dual_status_code in ('01','03','05','06') then 'Partial'
            else 'Non'
          end as dual_status
        /*
           The CMS-HCC model does not have factors for ESRD for these edge-cases,
           we default to 'Aged'. When OREC is missing, latest Medicare status is
           used, if available.
        */
        , case
            when original_reason_entitlement_code in ('0','2') then 'Aged'
            when original_reason_entitlement_code in ('1','3') then 'Disabled'
            when original_reason_entitlement_code is null and medicare_status_code in ('10','11','31') then 'Aged'
            when original_reason_entitlement_code is null and medicare_status_code in ('20','21') then 'Disabled'
            when coalesce(original_reason_entitlement_code,medicare_status_code) is null then 'Aged'
          end as orec
        /* Defaulting everyone to non-institutional until logic is added */
        , cast('No' as {{ dbt.type_string() }}) as institutional_status
        , enrollment_status_default
        , case
            when dual_status_code is null then TRUE
            else FALSE
          end as medicaid_dual_status_default
        /* Setting default true when OREC or Medicare Status is ESRD, or null */
        , case
            when original_reason_entitlement_code in ('2') then TRUE
            when original_reason_entitlement_code is null and medicare_status_code in ('31') then TRUE
            when coalesce(original_reason_entitlement_code,medicare_status_code) is null then TRUE
            else FALSE
          end as orec_default
        /* Setting default true until institutional logic is added */
        , TRUE as institutional_status_default
    from add_age_group

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(enrollment_status as {{ dbt.type_string() }}) as enrollment_status
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(age_group as {{ dbt.type_string() }}) as age_group
        , cast(medicaid_status as {{ dbt.type_string() }}) as medicaid_status
        , cast(dual_status as {{ dbt.type_string() }}) as dual_status
        , cast(orec as {{ dbt.type_string() }}) as orec
        , cast(institutional_status as {{ dbt.type_string() }}) as institutional_status
        , cast(enrollment_status_default as boolean) as enrollment_status_default
        , cast(medicaid_dual_status_default as boolean) as medicaid_dual_status_default
        , cast(orec_default as boolean) as orec_default
        , cast(institutional_status_default as boolean) as institutional_status_default
        , cast('{{ payment_year }}' as integer) as payment_year
    from add_status_logic

)

select
      patient_id
    , enrollment_status
    , gender
    , age_group
    , medicaid_status
    , dual_status
    , orec
    , institutional_status
    , enrollment_status_default
    , medicaid_dual_status_default
    , orec_default
    , institutional_status_default
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types