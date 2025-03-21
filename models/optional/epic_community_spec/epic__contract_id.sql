with cte as (
select  distinct a.payer
  , a.[plan] as [plan]
  , a.data_source
from {{ ref('core__eligibility') }} a
)

,final as (
select payer
,[plan] as [plan]
,data_source
,row_number() over (order by data_source,payer,[plan]) as  contract_id 
from cte
)

select payer
,[plan] as [plan]
,data_source
,cast(contract_id as varchar(4)) as contract_id
,concat([plan],data_source,payer) as contract_name
from final