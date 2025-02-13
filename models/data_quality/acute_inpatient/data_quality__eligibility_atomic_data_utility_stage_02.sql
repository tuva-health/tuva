{{ config(
    enabled = var('claims_enabled', False)
) }}

with eligibility as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
)

, missing_dual_status_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where dual_status_code is null

)

, missing_dual_status_code_perc as (
    select 
        round(
            (select eligibility_count from missing_dual_status_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, invalid_dual_status_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('terminology__medicare_dual_eligibility') }} bb
        on aa.dual_status_code = bb.dual_status_code
    where aa.dual_status_code is not null and bb.dual_status_code is null

)

, invalid_dual_status_code_perc as (
    select 
        round(
            (select eligibility_count from invalid_dual_status_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_dual_status_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct dual_status_code) as count_of_dual_status_codes
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct dual_status_code) > 1
    ) as subquery

)

, dupe_dual_status_code_perc as (
    select 
        round(
            (select eligibility_count from dupe_dual_status_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_medicare_status_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where medicare_status_code is null
)

, missing_medicare_status_code_perc as (
    select 
        round(
            (select eligibility_count from missing_medicare_status_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_medicare_status_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('terminology__medicare_status') }} bb
        on aa.medicare_status_code = bb.medicare_status_code
    where aa.medicare_status_code is not null and bb.medicare_status_code is null

)

, invalid_medicare_status_code_perc as (
    select 
        round(
            (select eligibility_count from invalid_medicare_status_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_medicare_status_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct medicare_status_code) as count_of_medicare_status_codes
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct medicare_status_code) > 1
    ) as subquery

)

, dupe_medicare_status_code_perc as (
    select 
        round(
            (select eligibility_count from dupe_medicare_status_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_first_name_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where first_name is null
)

, missing_first_name_perc as (
    select 
        round(
            (select eligibility_count from missing_first_name_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_first_name_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct first_name) as count_of_first_names
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct first_name) > 1
    ) as subquery
)

, dupe_first_name_perc as (
    select 
        round(
            (select eligibility_count from dupe_first_name_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_last_name_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where last_name is null
)

, missing_last_name_perc as (
    select 
        round(
            (select eligibility_count from missing_last_name_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_last_name_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct last_name) as count_of_last_names
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct last_name) > 1
    ) as subquery
)

, dupe_last_name_perc as (
    select 
        round(
            (select eligibility_count from dupe_last_name_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_social_security_number_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where social_security_number is null
)

, missing_social_security_number_perc as (
    select 
        round(
            (select eligibility_count from missing_social_security_number_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_social_security_number_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where (social_security_number is not null) and 
    (
        {% if target.type == 'fabric' %}
        len(social_security_number) != 9
        {% else %}
            length(social_security_number) != 9
        {% endif %}
    )
    
)

, invalid_social_security_number_perc as (
    select 
        round(
            (select eligibility_count from invalid_social_security_number_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_social_security_number_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct social_security_number) as count_of_social_security_numbers
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct social_security_number) > 1
    ) as subquery
)

, dupe_social_security_number_perc as (
    select 
        round(
            (select eligibility_count from dupe_social_security_number_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_address_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where address is null
)

, missing_address_perc as (
    select 
        round(
            (select eligibility_count from missing_address_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_address_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct address) as count_of_addresses
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct address) > 1
    ) as subquery
)

, dupe_address_perc as (
    select 
        round(
            (select eligibility_count from dupe_address_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_city_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where city is null
)

, missing_city_perc as (
    select 
        round(
            (select eligibility_count from missing_city_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_city_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct city) as count_of_cities
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct city) > 1
    ) as subquery
)

, dupe_city_perc as (
    select 
        round(
            (select eligibility_count from dupe_city_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_state_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where state is null
)

, missing_state_perc as (
    select 
        round(
            (select eligibility_count from missing_state_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_state_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('reference_data__ansi_fips_state') }} bb
        on aa.state = bb.ansi_fips_state_name
    where aa.state is not null and bb.ansi_fips_state_name is null
)

, invalid_state_perc as (
    select 
        round(
            (select eligibility_count from invalid_state_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_state_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct state) as count_of_states
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct state) > 1
    ) as subquery
)

, dupe_state_perc as (
    select 
        round(
            (select eligibility_count from dupe_state_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_zip_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where zip_code is null
)

, missing_zip_code_perc as (
    select 
        round(
            (select eligibility_count from missing_zip_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)


, invalid_zip_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    
    {% if target.type == 'fabric' %}

    where zip_code is not null and len(zip_code) not in (5,9,10)

    {% else %}
            
    where zip_code is not null and length(zip_code) not in (5,9,10)
        
    {% endif %}

)

, invalid_zip_code_perc as (
    select 
        round(
            (select eligibility_count from invalid_zip_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_zip_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct zip_code) as count_of_zip_codes
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct zip_code) > 1
    ) as subquery
)

, dupe_zip_code_perc as (
    select 
        round(
            (select eligibility_count from dupe_zip_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_phone_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where phone is null
)

, missing_phone_perc as (
    select 
        round(
            (select eligibility_count from missing_phone_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)  

, dupe_phone_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct phone) as count_of_phones
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct phone) > 1
    ) as subquery
)

, dupe_phone_perc as (
    select 
        round(
            (select eligibility_count from dupe_phone_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, final as (

    select
          'dual_status_code' as field
        , 'all' as claim_type
        , (select * from missing_dual_status_code_count) as missing_count
        , (select * from missing_dual_status_code_perc) as missing_perc
        , (select * from invalid_dual_status_code_count) as invalid_count
        , (select * from invalid_dual_status_code_perc) as invalid_perc
        , (select * from dupe_dual_status_code_count) as duplicated_count
        , (select * from dupe_dual_status_code_perc) as duplicated_perc

    union all

    select
          'medicare_status_code' as field
        , 'all' as claim_type
        , (select * from missing_medicare_status_code_count) as missing_count
        , (select * from missing_medicare_status_code_perc) as missing_perc
        , (select * from invalid_medicare_status_code_count) as invalid_count
        , (select * from invalid_medicare_status_code_perc) as invalid_perc
        , (select * from dupe_medicare_status_code_count) as duplicated_count
        , (select * from dupe_medicare_status_code_perc) as duplicated_perc

    union all

    select
          'first_name' as field
        , 'all' as claim_type
        , (select * from missing_first_name_count) as missing_count
        , (select * from missing_first_name_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_first_name_count) as duplicated_count
        , (select * from dupe_first_name_perc) as duplicated_perc
    
    union all

    select
          'last_name' as field
        , 'all' as claim_type
        , (select * from missing_last_name_count) as missing_count
        , (select * from missing_last_name_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_last_name_count) as duplicated_count
        , (select * from dupe_last_name_perc) as duplicated_perc

    union all

    select
          'social_security_number' as field
        , 'all' as claim_type
        , (select * from missing_social_security_number_count) as missing_count
        , (select * from missing_social_security_number_perc) as missing_perc
        , (select * from invalid_social_security_number_count) as invalid_count
        , (select * from invalid_social_security_number_perc) as invalid_perc
        , (select * from dupe_social_security_number_count) as duplicated_count
        , (select * from dupe_social_security_number_perc) as duplicated_perc

    union all

    select
          'address' as field
        , 'all' as claim_type
        , (select * from missing_address_count) as missing_count
        , (select * from missing_address_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_address_count) as duplicated_count
        , (select * from dupe_address_perc) as duplicated_perc

    union all

    select
          'city' as field
        , 'all' as claim_type
        , (select * from missing_city_count) as missing_count
        , (select * from missing_city_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_city_count) as duplicated_count
        , (select * from dupe_city_perc) as duplicated_perc

    union all

    select
          'state' as field
        , 'all' as claim_type
        , (select * from missing_state_count) as missing_count
        , (select * from missing_state_perc) as missing_perc
        , (select * from invalid_state_count) as invalid_count
        , (select * from invalid_state_perc) as invalid_perc
        , (select * from dupe_state_count) as duplicated_count
        , (select * from dupe_state_perc) as duplicated_perc

    union all

    select
          'zip_code' as field
        , 'all' as claim_type
        , (select * from missing_zip_code_count) as missing_count
        , (select * from missing_zip_code_perc) as missing_perc
        , (select * from invalid_zip_code_count) as invalid_count
        , (select * from invalid_zip_code_perc) as invalid_perc
        , (select * from dupe_zip_code_count) as duplicated_count
        , (select * from dupe_zip_code_perc) as duplicated_perc

    union all

    select
          'phone' as field
        , 'all' as claim_type
        , (select * from missing_phone_count) as missing_count
        , (select * from missing_phone_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_phone_count) as duplicated_count
        , (select * from dupe_phone_perc) as duplicated_perc

)

select
      'eligibility' as table_name
    , field
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
    , claim_type
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
