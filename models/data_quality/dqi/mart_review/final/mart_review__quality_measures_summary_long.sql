

select *
from {{ ref('quality_measures__summary_long') }} as s
