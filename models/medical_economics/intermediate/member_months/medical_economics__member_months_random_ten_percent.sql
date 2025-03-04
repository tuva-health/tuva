{{ config(
    materialized='incremental',
    unique_key='person_id'
) }}

with medical_claim as (

    select distinct 
          person_id
    from {{ ref('medical_economics__stg_core_member_months') }}

),

ten_percent_sample as (

    select *
    from medical_claim
    sample(10)

)

select 
      aa.*
    , current_timestamp as created_at
    , 'ten_percent_sample' as comparative_population
from {{ ref('medical_economics__stg_core_member_months') }} aa 
inner join ten_percent_sample bb 
    on aa.person_id = bb.person_id 


{% if is_incremental() %}
    -- For incremental models, we only want to insert new or modified data
    -- The condition ensures that only new rows since the last run are inserted
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}