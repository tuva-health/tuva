{{ config(
    enabled = var('claims_enabled', False)
) }}

with usable_rev_counts as (
    select
          claim_id
        , count(distinct revenue_center_code) as usable_rev_count
    from {{ ref('data_quality__rev_all') }}
    where valid_revenue_center_code = 1
    group by claim_id
)

, total_institutional_claims as (
    select
          total_claims
    from {{ ref('data_quality__calculated_claim_type_percentages') }}
    where calculated_claim_type = 'institutional'
)

, one_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 1
)

, two_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 2
)

, three_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 3
)

, four_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 4
)

, five_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 5
)

, six_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 6
)

, seven_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 7
)

, eight_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 8
)

, nine_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 9
)

, ten_or_more_rev_codes as (
    select
        count(distinct claim_id) as countdistinct
    from usable_rev_counts
    where usable_rev_count >= 10
)

, final as (

    select
        'Claims with >=1 usable rev code' as field
        , (select * from one_or_more_rev_codes) as number_of_claims
        , round(
            (select * from one_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=2 usable rev code' as field
        , (select * from two_or_more_rev_codes) as number_of_claims
        , round(
            (select * from two_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=3 usable rev code' as field
        , (select * from three_or_more_rev_codes) as number_of_claims
        , round(
            (select * from three_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=4 usable rev code' as field
        , (select * from four_or_more_rev_codes) as number_of_claims
        , round(
            (select * from four_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=5 usable rev code' as field
        , (select * from five_or_more_rev_codes) as number_of_claims
        , round(
            (select * from five_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=6 usable rev code' as field
        , (select * from six_or_more_rev_codes) as number_of_claims
        , round(
            (select * from six_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=7 usable rev code' as field
        , (select * from seven_or_more_rev_codes) as number_of_claims
        , round(
            (select * from seven_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=8 usable rev code' as field
        , (select * from eight_or_more_rev_codes) as number_of_claims
        , round(
            (select * from eight_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=9 usable rev code' as field
        , (select * from nine_or_more_rev_codes) as number_of_claims
        , round(
            (select * from nine_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

    union all

    select
        'Claims with >=10 usable rev code' as field
        , (select * from ten_or_more_rev_codes) as number_of_claims
        , round(
            (select * from ten_or_more_rev_codes) * 100.0 /
            (select * from total_institutional_claims), 1
        ) as percent_of_institutional_claims

)

select
      field
    , number_of_claims
    , percent_of_institutional_claims
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final