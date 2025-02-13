{{ config(
    enabled = var('claims_enabled', False)
) }}

with pharmacy_claims as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
)

, missing_person_id_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where person_id is null
)

, missing_person_id_perc as (
    select
        round(
            (select claim_count from missing_person_id_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, dupe_person_id_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct person_id) as count_of_person_ids
        from {{ ref('pharmacy_claim') }}
        group by claim_id
        having count(distinct person_id) > 1
    ) as subquery
)

, dupe_person_id_perc as (
    select
        round(
            (select claim_count from dupe_person_id_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, missing_prescribing_provider_npi_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where prescribing_provider_npi is null
)

, missing_prescribing_provider_npi_perc as (
    select round(
        (select * from missing_prescribing_provider_npi_count) * 100.0 /
        (select * from pharmacy_claims)
        , 1
    ) as percentage
)

, invalid_prescribing_provider_npi_count as (
    select count(distinct aa.claim_id) as claim_count
    from {{ ref('pharmacy_claim') }} aa
    left join {{ ref('terminology__provider') }} bb
        on aa.prescribing_provider_npi = bb.npi
    where aa.prescribing_provider_npi is not null 
        and bb.npi is null
)

, invalid_prescribing_provider_npi_perc as (
    select round(
        (select * from invalid_prescribing_provider_npi_count) * 100.0 /
        (select * from pharmacy_claims)
        , 1
    ) as percentage
)

, dupe_prescribing_provider_npi_count as (
    select cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id
            , count(distinct prescribing_provider_npi) as count_of_prescribing_provider_npis
        from {{ ref('pharmacy_claim') }}
        group by claim_id
        having count(distinct prescribing_provider_npi) > 1
    ) as subquery
)

, dupe_prescribing_provider_npi_perc as (
    select round(
        (select * from dupe_prescribing_provider_npi_count) * 100.0 /
        (select * from pharmacy_claims)
        , 1
    ) as percentage
)
, missing_dispensing_provider_npi_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where dispensing_provider_npi is null
)

, missing_dispensing_provider_npi_perc as (
    select round(
        (select * from missing_dispensing_provider_npi_count) * 100.0 /
        (select * from pharmacy_claims)
        , 1
    ) as percentage
)

, invalid_dispensing_provider_npi_count as (
    select count(distinct aa.claim_id) as claim_count
    from {{ ref('pharmacy_claim') }} aa
    left join {{ ref('terminology__provider') }} bb
        on aa.dispensing_provider_npi = bb.npi
    where aa.dispensing_provider_npi is not null 
        and bb.npi is null
)

, invalid_dispensing_provider_npi_perc as (
    select round(
        (select * from invalid_dispensing_provider_npi_count) * 100.0 /
        (select * from pharmacy_claims)
        , 1
    ) as percentage
)

, dupe_dispensing_provider_npi_count as (
    select cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id
            , count(distinct dispensing_provider_npi) as count_of_dispensing_provider_npis
        from {{ ref('pharmacy_claim') }}
        group by claim_id
        having count(distinct dispensing_provider_npi) > 1
    ) as subquery
)

, dupe_dispensing_provider_npi_perc as (
    select round(
        (select * from dupe_dispensing_provider_npi_count) * 100.0 /
        (select * from pharmacy_claims)
        , 1
    ) as percentage
)

, missing_dispensing_date_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where dispensing_date is null
)

, missing_dispensing_date_perc as (
    select 
        round(
            (select claim_count from missing_dispensing_date_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, invalid_dispensing_date_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.dispensing_date = bb.full_date
    where (aa.dispensing_date is not null and bb.full_date is null)
        or aa.dispensing_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date)
)

, invalid_dispensing_date_perc as (
    select 
        round(
            (select claim_count from invalid_dispensing_date_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, dupe_dispensing_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct dispensing_date) as count_of_dispensing_dates
        from {{ ref('pharmacy_claim') }}
        group by claim_id
        having count(distinct dispensing_date) > 1
    ) as subquery
)

, dupe_dispensing_date_perc as (
    select 
        round(
            (select claim_count from dupe_dispensing_date_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, missing_ndc_code_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where ndc_code is null
)

, missing_ndc_code_perc as (
    select 
        round(
            (select claim_count from missing_ndc_code_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, invalid_ndc_code_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }} aa
    left join {{ ref('terminology__ndc') }} bb
        on aa.ndc_code = bb.ndc
    where aa.ndc_code is not null and bb.ndc is null
)

, invalid_ndc_code_perc as (
    select 
        round(
            (select claim_count from invalid_ndc_code_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, missing_quantity_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where quantity is null
)

, missing_quantity_perc as (
    select 
        round(
            (select claim_count from missing_quantity_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, invalid_quantity_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where quantity < 0
)

, invalid_quantity_perc as (
    select 
        round(
            (select claim_count from invalid_quantity_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, missing_days_supply_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where days_supply is null
)

, missing_days_supply_perc as (
    select 
        round(
            (select claim_count from missing_days_supply_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, invalid_days_supply_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where days_supply < 0
        or days_supply > 90
)

, invalid_days_supply_perc as (
    select 
        round(
            (select claim_count from invalid_days_supply_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, missing_refills_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where refills is null
)

, missing_refills_perc as (
    select 
        round(
            (select claim_count from missing_refills_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, invalid_refills_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where refills < 0
)

, invalid_refills_perc as (
    select 
        round(
            (select claim_count from invalid_refills_count) * 100.0 /
            (select claim_count from pharmacy_claims), 1
        ) as percentage
)

, missing_paid_date_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('pharmacy_claim') }}
    where paid_date is null
)

, missing_paid_date_perc as (
    select 
        round(
        (select * from missing_paid_date_count) * 100.0 /
        (select * from pharmacy_claims), 1
        ) as percentage
)

, invalid_paid_date_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('pharmacy_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.paid_date = bb.full_date
    where (aa.paid_date is not null and bb.full_date is null)
        or (aa.paid_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))
)

, invalid_paid_date_perc as (
    select 
        round(
        (select * from invalid_paid_date_count) * 100.0 /
        (select * from pharmacy_claims), 1
        ) as percentage
)

, dupe_paid_date_count as (
    select 
        count(*) as claim_count
    from (
            select 
              aa.claim_id
            , count(distinct aa.paid_date) as count_of_paid_dates
            from {{ ref('pharmacy_claim') }} aa
            group by aa.claim_id
            having count(distinct aa.paid_date) > 1
        ) as subquery
)

, dupe_paid_date_perc as (
    select
        round(
        (select * from dupe_paid_date_count) * 100.0 /
        (select * from pharmacy_claims), 1
        ) as percentage
)

, missing_paid_amount_count as (
    select 
        count(distinct claim_id) as claim_count
    from (
            select 
              claim_id
            , max(paid_amount) as max_paid_amount
            from {{ ref('pharmacy_claim') }} 
            group by claim_id
        ) as subquery
    where max_paid_amount is null
)

, missing_paid_amount_perc as (
    select 
        round(
        (select * from missing_paid_amount_count) * 100.0 /
        (select * from pharmacy_claims), 1
        ) as percentage
)

, missing_allowed_amount_count as (
    select 
        count(distinct claim_id) as claim_count
    from (
            select 
            claim_id
            , max(allowed_amount) as max_allowed_amount
            from {{ ref('pharmacy_claim') }} 
            group by claim_id
        ) as subquery
    where max_allowed_amount is null
)

, missing_allowed_amount_perc as (
    select 
        round(
        (select * from missing_allowed_amount_count) * 100.0 /
        (select * from pharmacy_claims), 1
        ) as percentage
)

, final as (
    select
        'person_id' as field
        , (select claim_count from missing_person_id_count) as missing_count
        , (select percentage from missing_person_id_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select claim_count from dupe_person_id_count) as duplicated_count
        , (select percentage from dupe_person_id_perc) as duplicated_perc

    union all

    select
        'prescribing_provider_npi' as field
        , (select claim_count from missing_prescribing_provider_npi_count) as missing_count
        , (select percentage from missing_prescribing_provider_npi_perc) as missing_perc
        , (select claim_count from invalid_prescribing_provider_npi_count) as invalid_count
        , (select percentage from invalid_prescribing_provider_npi_perc) as invalid_perc
        , (select claim_count from dupe_prescribing_provider_npi_count) as duplicated_count
        , (select percentage from dupe_prescribing_provider_npi_perc) as duplicated_perc

    union all

    select
        'dispensing_provider_npi' as field
        , (select claim_count from missing_dispensing_provider_npi_count) as missing_count
        , (select percentage from missing_dispensing_provider_npi_perc) as missing_perc
        , (select claim_count from invalid_dispensing_provider_npi_count) as invalid_count
        , (select percentage from invalid_dispensing_provider_npi_perc) as invalid_perc
        , (select claim_count from dupe_dispensing_provider_npi_count) as duplicated_count
        , (select percentage from dupe_dispensing_provider_npi_perc) as duplicated_perc

    union all

    select
        'dispensing_date' as field
        , (select claim_count from missing_dispensing_date_count) as missing_count
        , (select percentage from missing_dispensing_date_perc) as missing_perc
        , (select claim_count from invalid_dispensing_date_count) as invalid_count
        , (select percentage from invalid_dispensing_date_perc) as invalid_perc
        , (select claim_count from dupe_dispensing_date_count) as duplicated_count
        , (select percentage from dupe_dispensing_date_perc) as duplicated_perc

    union all

    select
        'ndc_code' as field
        , (select claim_count from missing_ndc_code_count) as missing_count
        , (select percentage from missing_ndc_code_perc) as missing_perc
        , (select claim_count from invalid_ndc_code_count) as invalid_count
        , (select percentage from invalid_ndc_code_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc

    union all

    select
        'quantity' as field
        , (select claim_count from missing_quantity_count) as missing_count
        , (select percentage from missing_quantity_perc) as missing_perc
        , (select claim_count from invalid_quantity_count) as invalid_count
        , (select percentage from invalid_quantity_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc

    union all

    select
        'days_supply' as field
        , (select claim_count from missing_days_supply_count) as missing_count
        , (select percentage from missing_days_supply_perc) as missing_perc
        , (select claim_count from invalid_days_supply_count) as invalid_count
        , (select percentage from invalid_days_supply_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc

    union all

    select
        'refills' as field
        , (select claim_count from missing_refills_count) as missing_count
        , (select percentage from missing_refills_perc) as missing_perc
        , (select claim_count from missing_refills_count) as invalid_count
        , (select percentage from missing_refills_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc

    union all

    select
        'paid_date' as field
        , (select claim_count from missing_paid_date_count) as missing_count
        , (select percentage from missing_paid_date_perc) as missing_perc
        , (select claim_count from invalid_paid_date_count) as invalid_count
        , (select percentage from invalid_paid_date_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc

    union all

    select
        'paid_amount' as field
        , (select claim_count from missing_paid_amount_count) as missing_count
        , (select percentage from missing_paid_amount_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
)

select
      'pharmacy_claim' as table_name
    , field
    , 'all' as claim_type
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
from final
