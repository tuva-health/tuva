{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
/*
Steps for transforming eligibility data into member demographics:
    1) Determine enrollment status using Medicare Part B entitlement months
       within the collection year (CMS Medicare Managed Care Manual, Ch.7 p.11).
    2) Roll up to latest eligibility record for enrollment statuses.
    3) Add age groups based on the payment year.
    4) Determine other statuses.

Jinja is used to set payment year variable.
 - The payment_year var has been set here so it gets compiled.
 - CMS guidance: Age is calculated as of Feb 1 of the payment year.
 - The collection year is one year prior to the payment year.
*/

with persons as (

    /*
        Build the person set from claims to ensure we create
        person-month rows even before plan eligibility begins.
    */
    select distinct person_id
    from {{ ref('cms_hcc__stg_core__medical_claim') }}

)

, patient_death as (

    select person_id, death_date
    from {{ ref('cms_hcc__stg_core__patient') }}

)

, person_months as (

    select
          p.person_id
        , d.collection_year
        , d.payment_year
        , d.collection_start_date
        , d.collection_end_date
        , {{ concat_custom(['d.payment_year', "'-12-31'"]) }} as payment_year_end_date
    from persons p
    cross join {{ ref('cms_hcc__int_monthly_collection_dates') }} as d
    left join {{ ref('reference_data__calendar') }} as cal
        on cast(cal.last_day_of_month as date) = d.collection_end_date
    /*
        Cap months at the beneficiary's Medicare Part B enrollment start.
        If start is unknown, retain all months (existing default logic applies).
        Include the start month (use month end >= final_start), similar to death-month inclusion.
    */
    left join {{ ref('cms_hcc__int_medicare_enrollment_start') }} as start_cap
        on p.person_id = start_cap.person_id
    left join patient_death pd
        on p.person_id = pd.person_id
    /* include the death month; drop months strictly after death */
    where (pd.death_date is null or cal.first_day_of_month <= pd.death_date)
      and (start_cap.final_start is null or cal.last_day_of_month >= start_cap.final_start)

)

, stg_eligibility as (

    /*
        Join eligibility to person-months so months without plan
        eligibility are retained and filled via defaults.
    */
    select
          pm.person_id
        , elig.enrollment_start_date
        , elig.enrollment_end_date
        , elig.original_reason_entitlement_code
        , elig.dual_status_code
        , elig.medicare_status_code
        , pm.collection_year
        , pm.payment_year
        , pm.collection_start_date
        , pm.collection_end_date
        , pm.payment_year_end_date
        , row_number() over (
            partition by pm.person_id, pm.collection_end_date
            order by elig.enrollment_end_date desc
        ) as row_num /* used to dedupe eligibility */
    from person_months pm
    left join {{ ref('cms_hcc__stg_core__eligibility') }} as elig
        /* filter to eligibility spans overlapping the collection period */
        on pm.person_id = elig.person_id
        and elig.enrollment_start_date <= cast(pm.payment_year_end_date as date)
        and elig.enrollment_end_date >= pm.collection_start_date

)

, payment_year_age_dates as (

    select distinct
          payment_year
        , cast({{ concat_custom(['payment_year',"'-02-01'"]) }} as date) as payment_year_age_date
    from {{ ref('cms_hcc__int_monthly_collection_dates') }}

)

, stg_patient as (

    select
          patient.person_id
        , patient.sex
        , patient.birth_date
        , dates.payment_year
        , floor({{ datediff('birth_date', 'payment_year_age_date', 'year') }}) as payment_year_age
        , patient.death_date
    from {{ ref('cms_hcc__stg_core__patient') }} as patient
    cross join payment_year_age_dates as dates

)

, start as (

    /*
        CMS source: Chapter 7, page 11.
        "Operationally, CMS identifies new enrollees as those beneficiaries with less than
        12 months of Medicare Part B entitlement during the data collection year."
        We bring in the person-level Part B start (actual or inferred) to compute
        Part B months during the collection year for new vs continuing.
    */
    select person_id, final_start
    from {{ ref('cms_hcc__int_medicare_enrollment_start') }}
)

/* create proxy enrollment dates if outside of the collection year */
, cap_collection_start_end_dates as (

    select
          person_id
        , enrollment_start_date
        , enrollment_end_date
        , payment_year
        , collection_start_date
        , collection_end_date
        , case
            when enrollment_start_date < {{ try_to_cast_date('collection_start_date', 'YYYY-MM-DD') }}
            then {{ try_to_cast_date('collection_start_date', 'YYYY-MM-DD') }}
            else enrollment_start_date
          end as proxy_enrollment_start_date
        , case
            when enrollment_end_date > {{ try_to_cast_date('collection_end_date', 'YYYY-MM-DD') }}
            then {{ try_to_cast_date('collection_end_date', 'YYYY-MM-DD') }}
            else enrollment_end_date
          end as proxy_enrollment_end_date
    from stg_eligibility

)

, calculate_prior_coverage as (

    /*
        Compute Part B entitlement months in the collection year using
        Medicare Part B start. If Part B start is after the collection end,
        coverage_months = 0. Otherwise count months inclusively between
        max(PartBStart, collection_start_date) and collection_end_date.
    */
    select
          c.person_id
        , c.payment_year
        , c.collection_end_date
        , case
            when s.final_start is null then 0
            when s.final_start > c.collection_end_date then 0
            else ( {{ datediff(
                        'case when s.final_start > c.collection_start_date then s.final_start else c.collection_start_date end',
                        'c.collection_end_date',
                        'month') }} + 1 )
          end as coverage_months
        , ( {{ datediff('c.collection_start_date', 'c.collection_end_date', 'month') }} + 1 ) as collection_months
    from (
        /* use full person-month grid to allow pre-eligibility months */
        select distinct person_id, payment_year, collection_start_date, collection_end_date
        from person_months
    ) c
    left join start s
        on c.person_id = s.person_id

)

/*
   CMS guidance: A “New Enrollee” status is when a beneficiary has less than
   12 months of coverage prior to the payment year.
*/
, add_enrollment as (

    select
          person_id
        , payment_year
        , collection_end_date
        , case
            when coverage_months < collection_months then 'New'
            else 'Continuing'
          end as enrollment_status
    from calculate_prior_coverage

)

, latest_eligibility as (

    select
          pm.person_id
        , pm.payment_year
        , pm.collection_start_date
        , pm.collection_end_date
        , stg_patient.sex as gender
        , stg_patient.payment_year_age
        , elig.original_reason_entitlement_code
        , elig.dual_status_code
        , elig.medicare_status_code
        /* Defaulting to "New" enrollment status when missing */
        , case
            when add_enrollment.enrollment_status is null then 'New'
            else add_enrollment.enrollment_status
          end as enrollment_status
        {% if target.type == 'fabric' %}
            , case
                when add_enrollment.enrollment_status is null then 1
                else 0
              end as enrollment_status_default
        {% else %}
            , case
                when add_enrollment.enrollment_status is null then true
                else false
              end as enrollment_status_default
        {% endif %}
    from person_months pm
        left join (
            select *
            from stg_eligibility
            where row_num = 1
        ) as elig
            on pm.person_id = elig.person_id
            and pm.collection_end_date = elig.collection_end_date
        left outer join add_enrollment
            on pm.person_id = add_enrollment.person_id
            and pm.payment_year = add_enrollment.payment_year
            and pm.collection_end_date = add_enrollment.collection_end_date
        left outer join stg_patient
            on pm.person_id = stg_patient.person_id
            and pm.payment_year = stg_patient.payment_year

)

, elig_month_bounds as (

    /* First and last month where an eligibility record exists */
    select
          person_id
        , min(collection_end_date) as first_elig_month
        , max(collection_end_date) as last_elig_month
    from (
        select person_id, collection_end_date
        from stg_eligibility
        where enrollment_start_date is not null
          and row_num = 1
    ) x
    group by person_id

)

, elig_statuses as (

    /* Derive statuses only for months with an eligibility record */
    select
          le.person_id
        , le.collection_end_date
        , case
            when le.dual_status_code in ('01', '02', '03', '04', '05', '06', '08') then 'Yes'
            else 'No'
          end as medicaid_status
        , case
            when le.dual_status_code in ('02', '04', '08') then 'Full'
            when le.dual_status_code in ('01', '03', '05', '06') then 'Partial'
            else 'Non'
          end as dual_status
        , case
            when le.original_reason_entitlement_code in ('0', '2') then 'Aged'
            when le.original_reason_entitlement_code in ('1', '3') then 'Disabled'
            when le.original_reason_entitlement_code is null and le.medicare_status_code in ('10', '11', '31') then 'Aged'
            when le.original_reason_entitlement_code is null and le.medicare_status_code in ('20', '21') then 'Disabled'
            when coalesce(le.original_reason_entitlement_code, le.medicare_status_code) is null then 'Aged'
          end as orec
        , cast('No' as {{ dbt.type_string() }}) as institutional_status
    from latest_eligibility le
    where le.original_reason_entitlement_code is not null
       or le.dual_status_code is not null
       or le.medicare_status_code is not null

)

, first_known as (

    select s.person_id, s.medicaid_status, s.dual_status, s.orec, s.institutional_status
    from elig_statuses s
    inner join elig_month_bounds b
        on s.person_id = b.person_id
        and s.collection_end_date = b.first_elig_month

)

, last_known as (

    select s.person_id, s.medicaid_status, s.dual_status, s.orec, s.institutional_status
    from elig_statuses s
    inner join elig_month_bounds b
        on s.person_id = b.person_id
        and s.collection_end_date = b.last_elig_month

)

, add_age_group as (

    select
          person_id
        , payment_year
        , collection_start_date
        , collection_end_date
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
          ag.person_id
        , ag.payment_year
        , ag.collection_start_date
        , ag.collection_end_date
        , ag.enrollment_status
        , case
            when ag.gender = 'female' then 'Female'
            when ag.gender = 'male' then 'Male'
            else null
          end as gender
        , ag.age_group
        , case
            when ag.dual_status_code in ('01', '02', '03', '04', '05', '06', '08') then 'Yes'
            when b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month then fk.medicaid_status
            when b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month then lk.medicaid_status
            else 'No'
          end as medicaid_status
        , case
            when ag.dual_status_code in ('02', '04', '08') then 'Full'
            when ag.dual_status_code in ('01', '03', '05', '06') then 'Partial'
            when b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month then fk.dual_status
            when b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month then lk.dual_status
            else 'Non'
          end as dual_status
        /*
           The CMS-HCC model does not have factors for ESRD for these edge-cases,
           we default to 'Aged'. When OREC is missing, latest Medicare status is
           used, if available.
        */
        , case
            when ag.original_reason_entitlement_code in ('0', '2') then 'Aged'
            when ag.original_reason_entitlement_code in ('1', '3') then 'Disabled'
            when ag.original_reason_entitlement_code is null and ag.medicare_status_code in ('10', '11', '31') then 'Aged'
            when ag.original_reason_entitlement_code is null and ag.medicare_status_code in ('20', '21') then 'Disabled'
            when coalesce(ag.original_reason_entitlement_code, ag.medicare_status_code) is null and b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month then fk.orec
            when coalesce(ag.original_reason_entitlement_code, ag.medicare_status_code) is null and b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month then lk.orec
            else 'Aged'
          end as orec
        /* Defaulting everyone to non-institutional until logic is added */
        , case
            when b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month then fk.institutional_status
            when b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month then lk.institutional_status
            else cast('No' as {{ dbt.type_string() }})
          end as institutional_status
        , ag.enrollment_status_default
        , case
            {% if target.type == 'fabric' %}
                when ag.dual_status_code is null and (
                    not (
                        (b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month)
                        or (b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month)
                    )
                ) then 1 else 0
            {% else %}
                when ag.dual_status_code is null and (
                    not (
                        (b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month)
                        or (b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month)
                    )
                ) then true else false
            {% endif %}
          end as medicaid_dual_status_default
        /* Setting default true when OREC or Medicare Status is ESRD, or null */
        , case
            {% if target.type == 'fabric' %}
                when (
                    (b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month)
                    or (b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month)
                ) then 0
                when ag.original_reason_entitlement_code in ('2') then 1
                when ag.original_reason_entitlement_code is null and ag.medicare_status_code in ('31') then 1
                when coalesce(ag.original_reason_entitlement_code,ag.medicare_status_code) is null then 1
                else 0
            {% else %}
                when (
                    (b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month)
                    or (b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month)
                ) then false
                when ag.original_reason_entitlement_code in ('2') then true
                when ag.original_reason_entitlement_code is null and ag.medicare_status_code in ('31') then true
                when coalesce(ag.original_reason_entitlement_code, ag.medicare_status_code) is null then true
                else false
            {% endif %}
          end as orec_default
        /* Institutional default: no source data yet; always default */
        {% if target.type == 'fabric' %}
            , 1 as institutional_status_default
        {% else %}
            , true as institutional_status_default
        {% endif %}
        , case
            when (b.first_elig_month is not null and ag.collection_end_date < b.first_elig_month)
              or (b.last_elig_month is not null and ag.collection_end_date > b.last_elig_month)
            then 1 else 0 end as eligibility_imputed_int
    from add_age_group as ag
        left join elig_month_bounds b on ag.person_id = b.person_id
        left join first_known fk on ag.person_id = fk.person_id
        left join last_known lk on ag.person_id = lk.person_id

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(enrollment_status as {{ dbt.type_string() }}) as enrollment_status
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(age_group as {{ dbt.type_string() }}) as age_group
        , cast(medicaid_status as {{ dbt.type_string() }}) as medicaid_status
        , cast(dual_status as {{ dbt.type_string() }}) as dual_status
        , cast(orec as {{ dbt.type_string() }}) as orec
        , cast(institutional_status as {{ dbt.type_string() }}) as institutional_status
        {% if target.type == 'fabric' %}
            , cast(enrollment_status_default as bit) as enrollment_status_default
            , cast(medicaid_dual_status_default as bit) as medicaid_dual_status_default
            , cast(orec_default as bit) as orec_default
            , cast(institutional_status_default as bit) as institutional_status_default
            , cast(eligibility_imputed_int as bit) as eligibility_imputed
        {% else %}
            , cast(enrollment_status_default as boolean) as enrollment_status_default
            , cast(medicaid_dual_status_default as boolean) as medicaid_dual_status_default
            , cast(orec_default as boolean) as orec_default
            , cast(institutional_status_default as boolean) as institutional_status_default
            , cast(case when eligibility_imputed_int = 1 then true else false end as boolean) as eligibility_imputed
        {% endif %}
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from add_status_logic

)

select
      person_id
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
    , eligibility_imputed
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
