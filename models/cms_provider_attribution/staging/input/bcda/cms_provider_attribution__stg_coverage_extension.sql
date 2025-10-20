{{ config(
     enabled = var('claims_preprocessing_enabled', False) and var('attribution_claims_source') == 'bcda'
 | as_bool)
}}

with union_cte as 
(
    select *
    from {{ source('phds_lakehouse_test','yak_bcda_coverage_extension') }}
)

,cte as (
select *
,row_number() over (partition by coverage_id, url order by file_date desc)  as most_recent_record
from union_cte
)

select *
from cte
where most_recent_record = 1

