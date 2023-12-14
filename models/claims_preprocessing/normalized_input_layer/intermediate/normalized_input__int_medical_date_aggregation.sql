with terminology_calendar as(
    select 
        min(full_date) as minimum_calendar_date
        , max(full_date) as maximum_calendar_date
    from {{ ref('terminology__calendar') }}
)
, claim_dates as(
    select
        claim_id
        , data_source
        , min(claim_start_date) as minimum_claim_start_date
        , max(claim_end_date) as maximum_claim_end_date
        , min(admission_date) as minimum_admission_date
        , max(claim_end_date) as maximum_discharge_date
    from {{ ref('medical_claim') }}
    where claim_type = 'institutional'
    group by
        claim_id
        , data_source

    union all

    select
        claim_id
        , data_source
        , min(claim_start_date) as minimum_claim_start_date
        , max(claim_end_date) as maximum_claim_end_date
        , null as minimum_admission_date
        , null as maximum_discharge_date
    from {{ ref('medical_claim') }}
    where claim_type = 'professional'
    group by
        claim_id
        , data_source
) 

select
    claim_id
    , data_source
    , case 
        when minimum_claim_start_date > minimum_calendar_date or minimum_claim_start_date < maximum_calendar_date
            then minimum_claim_start_date
    end as minimum_claim_start_date
    , case 
        when maximum_claim_end_date > minimum_calendar_date or maximum_claim_end_date < maximum_calendar_date
            then maximum_claim_end_date
    end as maximum_claim_end_date
    , case 
        when minimum_admission_date > minimum_calendar_date or minimum_admission_date < maximum_calendar_date
            then minimum_admission_date
    end as minimum_admission_date
    , case 
        when maximum_discharge_date > minimum_calendar_date or maximum_discharge_date < maximum_calendar_date
            then maximum_discharge_date
    end as maximum_discharge_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from claim_dates
join terminology_calendar s