
-- This dbt model has the following columns:
--     venn_section
--     claim_count

-- If we draw a Venn diagram of the 3 requirements that may be
-- relevant to tag institutional claims as 'acute inpatient' claims:
--      The Room & Board Requirement
--      The DRG Requirement
--      The Bill Type Requirement
-- Then we have a Venn diagram of 3 intersecting sets,
-- yielding 2^3 distinct areas.
-- This dbt model tells us how many claims fall into each of those areas
-- (with the exception of the null set: the number of claims that don't meet
--  any of the 3 requirements).
-- So this table always has exactly 7 rows:
--     rb
--     drg
--     bill
--     rb_drg
--     rb_bill
--     drg_bill
--     rb_drg_bill


with rb as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 0 and bill = 0
),


drg as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 0 and drg = 1 and bill = 0
),


bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 0 and drg = 0 and bill = 1
),


rb_drg as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 1 and bill = 0
),


rb_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 0 and bill = 1
),


drg_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 0 and drg = 1 and bill = 1
),


rb_drg_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 1 and bill = 1
),


summary_cte as (

select 
  'rb' as venn_section,
  (select * from rb) as claim_count

union all

select 
  'drg' as venn_section,
  (select * from drg) as claim_count

union all

select 
  'bill' as venn_section,
  (select * from bill) as claim_count

union all

select 
  'rb_drg' as venn_section,
  (select * from rb_drg) as claim_count

union all

select 
  'rb_bill' as venn_section,
  (select * from rb_bill) as claim_count

union all

select 
  'drg_bill' as venn_section,
  (select * from drg_bill) as claim_count

union all

select 
  'rb_drg_bill' as venn_section,
  (select * from rb_drg_bill) as claim_count

)


select *
from summary_cte
