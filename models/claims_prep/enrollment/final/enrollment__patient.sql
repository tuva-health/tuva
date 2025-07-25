with enrollment__stg_eligibility as (
    select *
    from {{ ref('the_tuva_project', 'enrollment__stg_eligibility') }}
)
, most_recent_eligibility as (
    {{ dbt_utils.deduplicate(
        relation='enrollment__stg_eligibility',
        partition_by='data_source, member_id',
        order_by='enrollment_start_date desc',
        )
    }}
)
select
    {{ dbt_utils.generate_surrogate_key(['data_source', 'member_id']) }} as patient_sk
    , data_source
    , member_id
    , first_name
    , cast(null as {{ dbt.type_string() }}) as middle_name
    , last_name
    , cast(null as {{ dbt.type_string() }}) as suffix
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , phone
    , cast(null as {{ dbt.type_string() }}) as email
from most_recent_eligibility