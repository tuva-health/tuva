{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
    person_id
    , payer
    , data_source
    , suspect_date as recorded_date
    , model_version
    , null as claim_id
    , hcc_code
    , hcc_description
    , 1 as suspect_hcc_flag
    , 1 as eligible_claim_flag
    , 'suspect' as hcc_type
    , 'payer' as hcc_source
from {{ ref('hcc_suspecting__list_all') }}
-- Exclude since already included in int_all_conditions
where lower(reason) != 'prior coding history'

{% if var('hcc_recapture_suspect_list') %}
union all
select
    *
from {{ ref('hcc_recapture__stg_suspect_hccs')}}
{% endif %}