{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select distinct
    person_id
    , payer
    , data_source
    , suspect_date as recorded_date
    , model_version
    , null as claim_id
    , hcc_code
    , hcc_description
    , 0 as external_hcc_flag
    , 1 as eligible_claim_flag
    , reason
    , 'suspect' as hcc_type
    , 'payer' as hcc_source
from {{ ref('hcc_suspecting__list_all') }}
-- Exclude since already included in int_all_conditions
where lower(reason) != 'prior coding history'

{% if var('hcc_recapture_external_suspect_list', false) | as_bool %}
union all

select distinct
    person_id
    , payer
    , data_source
    , recorded_date
    , model_version
    , claim_id
    , hcc_code
    , hcc_description
    , 1 external_hcc_flag
    , 1 eligible_claim_flag
    , reason
    , hcc_type
    , hcc_source
from {{ ref('hcc_recapture__stg_suspect_hccs')}}
{% endif %}
