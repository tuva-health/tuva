select
    person_id
    , data_source
    , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as patient_source_key
    ,year_month
    ,payer
    ,{{ quote_column('plan') }}
    ,count(1) as member_months
from {{ ref('core__member_months') }}
group by
    person_id
    ,data_source
    ,year_month
    ,payer
    ,{{ quote_column('plan') }}