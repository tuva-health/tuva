{{ config(
     enabled = var('hcc_recapture_suspect_list') | as_bool
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
    , cast(suspect_hcc_flag as {{dbt.type_int()}}) as suspect_hcc_flag
    , cast(eligible_claim_flag as {{dbt.type_int()}}) as eligible_claim_flag
    , cast(hcc_type as {{dbt.type_string()}}) as hcc_type
    , cast(hcc_source as {{dbt.type_string()}}) as hcc_source
from {{ ref('suspect_hccs')}}
