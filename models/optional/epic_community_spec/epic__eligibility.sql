select
     cast(e.person_id as varchar(50)) as [Person ID]
   , cast(e.member_id as varchar(200)) as [Member Number]
   , 0 as [Adjustment Type Code]
   , 0 as [Adjustment Sequence]
   , cast(null as int) as [Relationship Code Set]
   , cast(null as varchar(2)) as [Relationship Code]
   , convert(varchar(8), e.enrollment_start_date, 112) as [Coverage Effective Date]
   , case
         when e.enrollment_end_date is not null then convert(varchar(8), e.enrollment_end_date, 112)
         else null
     end as [Coverage Termination Date]
   , cast(e.group_id as varchar(50)) as [Group Number]
   , cast(null as varchar(50)) as [Group Code]
   , cast(null as varchar(100)) as [Group Name]
   , cast(null as varchar(50)) as [Plan Type]
   , cast(contract_id as varchar(50)) as [Benefit Plan ID]
   , cast(contract_name as varchar(100)) as [Benefit Plan Name]
   , cast(null as varchar(100)) as [Region Name]
   , cast(e.payer_type as varchar(100)) as [Line of Business Name]
   , cast(null as varchar(100)) as [Corporation Name]
   , cast(e.payer as varchar(100)) as [Payer Name]
   , cast(c.contract_id as varchar(50)) as [Contract ID]
   , cast(null as varchar(50)) as [Patient Primary Location ID]
   , cast(null as varchar(200)) as [Enrollment Category]
   , cast(e.subscriber_id as varchar(50)) as [Subscriber ID]
from {{ ref('core__eligibility') }} e
inner join {{ ref('epic__contract_id') }} c
    on e.data_source = c.data_source
   and e.payer = c.payer
   and e.[plan] = c.[plan]
