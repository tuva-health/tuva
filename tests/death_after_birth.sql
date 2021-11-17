

select *
from {{ ref('stg_patients') }}
where (deceased_date is not null) and (birth_date > deceased_date)