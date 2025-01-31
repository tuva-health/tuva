with provider_distinct as (

    select distinct
          primary_specialty_description as specialty_provider
    from {{ ref('terminology__provider') }}

),

provider as (

    select 
          specialty_provider
        , row_number() over (
            order by specialty_provider
        ) as specialty_provider_id
    from provider_distinct

)

select * 
from provider 