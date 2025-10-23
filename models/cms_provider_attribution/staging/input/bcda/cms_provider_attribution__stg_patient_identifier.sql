{{ config(
     enabled = var('cms_provider_attribution_enabled', False) and var('attribution_claims_source') == 'bcda'
 | as_bool)
}}

with union_cte as (
    select 
        *
    from {{ source('phds_lakehouse_test','yak_bcda_patient_identifier') }}
    
)
,cte as (
    select *
         , row_number() over (
             partition by patient_id, value
             order by file_date desc
           ) as most_recent_record
    from union_cte
)

select *
from cte
where most_recent_record = 1