select distinct
    left(e.year_month, 4) as year_number
  , e.patient_id
  , e.data_source
  , datediff('year', p.birth_date, to_date(e.year_month, 'YYYYMM')) as age
from {{ ref('financial_pmpm__member_months') }} as e
inner join {{ ref('core__patient') }} as p 
  on e.patient_id = p.patient_id
  and p.data_source = e.data_source
where datediff('year', p.birth_date, to_date(e.year_month, 'YYYYMM')) >= 40
