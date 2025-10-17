with base as (
{% if var('assignment_methodology') == 'retrospective' %}

select retro.*, 'retrospective' as assignment_methodology from {{ref('cms_provider_attribution__stg_aalr1_retrospective')}} retro

{% elif var('assignment_methodology') == 'prospective' %}

select prosp.*, 'prospective' as assignment_methodology from {{ref('cms_provider_attribution__stg_aalr1_prospective')}} prosp

{% endif %}
)

, aalr as (
select 
    SUBSTRING(file_name, 3,5) as aco_id
  , cast((case 
      when CHARINDEX('QALR.', file_name) != 0 then SUBSTRING(file_name, CHARINDEX('QALR.', file_name) + 5, 4)
      when CHARINDEX('AALR.', file_name) != 0 then SUBSTRING(file_name, CHARINDEX('AALR.', file_name) + 5 + 1, 4)
    end) as int) as performance_year
  , case 
      when file_name like '%Q1%' then 1
      when file_name like '%Q2%' then 2
      when file_name like '%Q3%' then 3 
      when file_name like '%Q4%' then 4
      when file_name like '%Y2%' then 5
      else 0
    end as quarter    
  , base.* 
from base 
)
-- Ensure the ACO AALR is loaded from the proper source with the correct fields
select 
  aalr.* 
from aalr
inner join {{ref('cms_provider_attribution__stg_assignment_methodology')}} asgn
    on  aalr.aco_id = asgn.aco_id
    and aalr.performance_year = asgn.performance_year
    and aalr.assignment_methodology = asgn.assignment_methodology