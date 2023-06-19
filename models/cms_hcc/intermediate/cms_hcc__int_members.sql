{{ config(
     enabled = var('cms_hcc_enabled',var('tuva_marts_enabled',True))
   )
}}
/*
Steps for transforming eligibility data into member demographics:
    1) Determine enrollment status using eligibility from the collection year.
    2) Roll up to latest eligibility record.
    3) Add age groups based on the payment year.
    4) Determine other statuses.

Jinja is used to set payment and collection year variables.
 - The hcc_model_version and payment_year vars have been set here
   so they get compiled.
 - CMS guidance: Age is calculated as of Feb 1 of the payment year.
 - The collection year is one year prior to the payment year.
*/

{% set model_version_compiled = var('cms_hcc_model_version') -%}
{% set payment_year_compiled = var('cms_hcc_payment_year') -%}
{% set payment_year_age_date = payment_year_compiled ~ '-02-01' -%}
{% set collection_year = payment_year_compiled - 1 -%}
{% set collection_year_start = collection_year ~ '-01-01' -%}

with stg_eligibility as (

    select
          patient_id
        , gender
        , birth_date
        , floor({{ datediff('birth_date', "'"~payment_year_age_date~"'", 'hour') }} / 8766.0) as payment_year_age
        , enrollment_start_date
        , enrollment_end_date
        , dual_status_code
        , medicare_status_code
        , row_number() over(
            partition by patient_id
            order by enrollment_end_date desc
        ) as row_num /* used to dedupe eligibility */
    from {{ ref('cms_hcc__stg_eligibility') }}

)

, calculate_prior_coverage as (

    select
          patient_id
        , enrollment_start_date
        , case
            when enrollment_start_date < '{{ collection_year_start }}'
            then '{{ collection_year_start }}'
            else enrollment_start_date
          end as proxy_enrollment_start_date
        , enrollment_end_date
        , case
            when enrollment_start_date < '{{ collection_year_start }}'
            then {{ datediff("'"~collection_year_start~"'", 'enrollment_end_date', 'month') }} +1 /* include starting month */
            else {{ datediff('enrollment_start_date', 'enrollment_end_date', 'month') }} +1  /* include starting month */
          end as coverage_months
    from stg_eligibility
    where
    /* coverage dates must fall within the collection year */
    (extract(year from enrollment_start_date) = {{ collection_year }}
     or extract(year from enrollment_start_date) = {{ collection_year }})

)

/*
   CMS guidance: A “New Enrollee” status is when a beneficiary has less than
   12 months of coverage prior to the payment year.
*/
, add_enrollment as (

    select distinct
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
        , stg_eligibility.gender
        , stg_eligibility.payment_year_age
        , stg_eligibility.dual_status_code
        , stg_eligibility.medicare_status_code
        , case
            when add_enrollment.enrollment_status is null then 'New'
            else add_enrollment.enrollment_status
          end as enrollment_status
        , case
            when add_enrollment.enrollment_status is null then True
            else False
          end as enrollment_status_default
    from stg_eligibility
         left join add_enrollment
         on stg_eligibility.patient_id = add_enrollment.patient_id
    where stg_eligibility.row_num = 1

)

, add_age_group as (

    select
          patient_id
        , gender
        , payment_year_age
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

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(enrollment_status as {{ dbt.type_string() }}) as enrollment_status
        /*, null as plan_segment --data not available */
        , cast(case
            when gender = 'female' then 'Female'
            when gender = 'male' then 'Male'
            else null
          end as {{ dbt.type_string() }}) as gender
        , cast(age_group as {{ dbt.type_string() }}) as age_group
        , cast(case
            when dual_status_code in ('01','02','03','04','05','06','08') then 'Yes'
            else 'No'
          end as {{ dbt.type_string() }}) as medicaid_status
        , cast(case
            when dual_status_code in ('02','04','08') then 'Full'
            when dual_status_code in ('01','03','05','06') then 'Partial'
            else 'Non'
          end as {{ dbt.type_string() }}) as dual_status
        /*
           Medicare status is being used as an analog for OREC to calculate
           demographic risk factors, this will be replaced when OREC is added to
           the data model.
        */
        , cast(case
            when medicare_status_code in ('10','11') then 'Aged'
            when medicare_status_code in ('20','21') then 'Disabled'
            when medicare_status_code in ('31') then 'ESRD'
            end as {{ dbt.type_string() }}) as orec
        /*
           Defaulting everyone to non-institutional until logic is added
        */
        , cast('No'as {{ dbt.type_string() }}) as institutional_status
        , cast(enrollment_status_default as boolean) as enrollment_status_default
        , cast(case
            when dual_status_code is null then True
            else FALSE
            end as boolean) as medicaid_dual_status_default
        , cast(True as boolean) as institutional_status_default
        , cast('{{ model_version_compiled }}' as {{ dbt.type_string() }}) as model_version
        , cast('{{ payment_year_compiled }}' as integer) as payment_year
        , cast('{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as {{ dbt.type_timestamp() }}) as date_calculated
    from add_age_group

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
    , institutional_status_default
    , model_version
    , payment_year
    , '{{ var('last_update')}}' as last_update
from add_data_types