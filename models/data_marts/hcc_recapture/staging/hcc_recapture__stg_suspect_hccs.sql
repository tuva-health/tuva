{{ config(
     enabled = var('hcc_recapture_external_suspect_list', false) | as_bool
   )
}}

select
    cast(person_id as {{dbt.type_string()}}) as person_id
    , cast(payer as {{dbt.type_string()}}) as payer
    , cast(data_source as {{dbt.type_string()}}) as data_source
    , cast(recorded_date as date) as recorded_date
    , cast(model_version as {{dbt.type_string()}}) as model_version
    , cast(claim_id as {{dbt.type_string()}}) as claim_id
    , cast(hcc_code as {{dbt.type_string()}}) as hcc_code
    , cast(hcc_description as {{dbt.type_string()}}) as hcc_description
    , cast(reason as {{dbt.type_string()}}) as reason
    , cast(hcc_type as {{dbt.type_string()}}) as hcc_type
    , cast(hcc_source as {{dbt.type_string()}}) as hcc_source
from {{ ref('suspect_hccs')}}
