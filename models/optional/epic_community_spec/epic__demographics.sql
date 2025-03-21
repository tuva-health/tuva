select 
     cast(person_id as varchar(50)) as [Person ID]
   , cast(first_name as varchar(100)) as [First Name]
   , cast(last_name as varchar(100)) as [Last Name]
   , cast(null as varchar(100)) as [Middle Name]
   , cast(null as varchar(10)) as [Name Suffix]
   , cast(social_security_number as varchar(11)) as [SSN]
   , convert(varchar(8), birth_date, 112) as [Date of Birth]
   , case
         when death_date is not null then convert(varchar(8), death_date, 112)
         else null
     end as [Date of Death]
   , case
         when lower(sex) in ('female', 'f') then 1
         when lower(sex) in ('male', 'm') then 2
         when lower(sex) = 'other' then 3
         else null
     end as [Sex]
   , cast(null as varchar(150)) as [Email Address]
   , cast(address as varchar(100)) as [Address]
   , cast(null as varchar(100)) as [Address Line 2]
   , cast(city as varchar(100)) as [City]
   , cast(state as varchar(2)) as [State]
   , cast(zip_code as varchar(10)) as [ZIP Code]
   , cast(phone as varchar(14)) as [Phone Number]
from {{ ref('core__patient') }}
