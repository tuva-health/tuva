
with date_bounds as (
    select
        date_trunc('month', min(date_col)) as min_month,
        date_trunc('month', max(date_col)) as max_month
    from {{ ref('cms_hcc__stg_core__medical_claim') }}
)

, date_spine as (
    {{ dbt_utils.date_spine(
    datepart="month",
    start_date="'(select min_month from date_bounds)'",
    -- Date spine is exclusive, so need to add 1 month to include the final month
    end_date="(select dateadd(month, 1, max_month) from date_bounds)"
) }}
)

-- Details on implementation can be found here: https://www.cms.gov/files/document/medicare-shared-savings-program-shared-savings-and-losses-and-assignment-methodology-specifications.pdf
, covid_episodes_base as (
    select
          claim_id
        , person_id
        , date_trunc('month', admission_date) as start_date
        -- Add 2 months since we need the last day of the next month
        , date_trunc('month', dateadd(month, 2, discharge_date)) - 1 as end_date

    from {{ ref('cms_hcc__stg_core__medical_claim') }}
    where claim_type = 'institutional'
        and substring(bill_type_code, 1, 2) in ('11', '41')
        and admission_date is not null 
        and discharge_date is not null
        and (
            (discharge_date between '2020-01-27' and '2021-03-31' and code = 'B9729')
            or
            -- End of PHE: https://www.cms.gov/priorities/health-equity/minority-health/resource-center/moving-forward-after-covid-19-public-health-emergency
            (discharge_date between '2020-04-01' and '2023-05-11' and code = 'U071')
        )
        -- TODO: Bring in CCN from the CCLF files in the medical_claim input
        -- and (
        --     substring(ccn,3,1) in ('T', 'R')
        --     or
        --     (
        --         right(cast(ccn as int),4) between 1 and 879
        --         or 
        --         right(cast(ccn as int),4) between 1300 and 1399
        --         or
        --         right(cast(ccn as int),4) between 2000 and 2299
        --         or
        --         right(cast(ccn as int),4) between 3025 and 3099
        --         or
        --         right(cast(ccn as int),4) between 3300 and 3399
        --     )
        -- )
)

, covid_episodes as (
    select distinct
          cast(covid.person_id as {{ dbt.type_string() }}) as person_id
        , cast(date_spine.date_spine as date) as yearmo_trunc
    
    from covid_episodes_base covid
    inner join date_spine
        on date_spine.date_spine between covid.start_date and covid.end_date
)

select
      person_id
    , yearmo_trunc
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from covid_episodes