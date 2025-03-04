{{ config(
    materialized='incremental',
    unique_key='person_id'
) }}

with member_months as (

    select distinct 
          person_id
    from {{ ref('medical_economics__stg_core_member_months') }}

),

twenty_percent_sample as (

    select *
    from member_months
    sample(20)

)

select 
      aa.*
    , current_timestamp as created_at
    , 'twenty_percent_sample' as comparative_population
from {{ ref('medical_economics__stg_core_member_months') }} aa 
inner join twenty_percent_sample bb 
    on aa.person_id = bb.person_id 


{% if is_incremental() %}
    -- For incremental models, we only want to insert new or modified data
    -- The condition ensures that only new rows since the last run are inserted
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}