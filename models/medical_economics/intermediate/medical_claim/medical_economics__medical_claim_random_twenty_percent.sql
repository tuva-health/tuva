{{ config(
    materialized='incremental',
    unique_key='claim_id'
) }}

with medical_claim as (

    select distinct 
          claim_id
    from {{ ref('medical_economics__stg_core_medical_claim') }}

),

ten_percent_sample as (

    select *
    from medical_claim
    sample(20)

)

select 
      aa.*
    , current_timestamp as created_at
    , 'twenty_percent_sample' as comparative_population
from {{ ref('medical_economics__stg_core_medical_claim') }} aa 
inner join ten_percent_sample bb 
    on aa.claim_id = bb.claim_id 


{% if is_incremental() %}
    -- For incremental models, we only want to insert new or modified data
    -- The condition ensures that only new rows since the last run are inserted
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}