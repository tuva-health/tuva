
{{ config(materialized='view') }}

select
    cast(member_id as string) as patient_id
,   case
        when cast(gender_code as string) = 'male' then 1
        when cast(gender_code as string) = 'female' then 0
        else 2
    end gender_code
,   to_date(birth_date) as birth_date
,   to_date(deceased_date) as deceased_date
from hcup.public.members