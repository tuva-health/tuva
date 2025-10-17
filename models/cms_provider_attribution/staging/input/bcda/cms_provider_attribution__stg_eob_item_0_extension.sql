with union_cte as (
    select *
         , 'main' as runout_type
    from {{ source('phds_lakehouse_test','yak_bcda_explanationofbenefit_item_0_extension') }}

)
,cte as (
    select *
         , row_number() over (
             partition by eob_id, url
             order by file_date desc
           ) as most_recent_record
    from union_cte
)

select *
from cte
where most_recent_record = 1
