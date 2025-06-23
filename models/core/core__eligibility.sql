
select
    surrogate_key as eligibility_id
    , person_id
    , member_id
    , subscriber_id
    , birth_date
    , death_date
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ quote_column('plan') }}
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , subscriber_relation
    , group_id
    , group_name
    , data_source
    , cast('{{ current_timestamp() }}' as {{ dbt.type_string() }}) as tuva_last_run
from {{ ref('the_tuva_project', 'normalized_input__eligibility') }}