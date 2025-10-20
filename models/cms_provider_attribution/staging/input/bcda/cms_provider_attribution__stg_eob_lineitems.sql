{{ config(
     enabled = var('claims_preprocessing_enabled', False) and var('attribution_claims_source') == 'bcda'
 | as_bool)
}}

with union_cte as (
    select *
         , 'main' as runout_type
    from {{ source('phds_lakehouse_test','yak_bcda_explanationofbenefit_lineitems') }}


)
,cte as (
    select *
         , row_number() over (
             partition by header_id, anchor_sequence
             order by header_created desc, file_date desc
           ) as most_recent_record
    from union_cte
)

select *
from cte
where most_recent_record = 1