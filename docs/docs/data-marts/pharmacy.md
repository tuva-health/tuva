---
id: pharmacy
title: "Pharmacy"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/pharmacy/)

Understanding pharmacy spend in healthcare data is crucial for identifying cost trends and optimizing medication management. It allows healthcare providers and payers to track drug utilization, assess the efficacy of formulary decisions, and implement strategies to enhance patient care while controlling costs.

Currently, the pharmacy mart contains the brand and generic retail pharmacy analysis. This runs on claims data and provides an easy, out-of-the-box way to identify which brand drugs members were prescribed to members when a generic alternative was available. It also calculates what the dollar savings would be when switching to a generic, based on historical generic prices in your claims history.

There are 3 final tables in the pharmacy mart:

1. **brand_generic_opportunity**  
   This table calculates the potential dollar savings when switching from a brand drug to an available generic. It operates at the claim line level.

2. **pharmacy_claim_expanded**  
   This table reproduces the pharmacy_claim table with the additional fields produced by the brand generic analysis.

3. **generic_available_list**  
   This table lists all available generics at the NDC code level for each brand drug. It can be joined with the pharmacy_claim_expanded table using the generic_available_sk field.

The mart creates and uses the following seed files:
- terminology.rxnorm_brand_generic
- pharmacy.rxnorm_generic_available

## Example SQL

### Pharmacy Claims and Enrollment

<details>
  <summary>Members with Pharmacy Claims by Month</summary>

```sql
with pharmacy_claim as 
(
select 
  data_source
  , person_id
  , to_char(paid_date, 'YYYYMM') AS year_month
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.pharmacy_claim
GROUP BY data_source
, person_id
, to_char(paid_date, 'YYYYMM')
)

select mm.data_source
, mm.year_month
, sum(case when mc.person_id is not null then 1 else 0 end) as members_with_claims
, count(*) as total_member_months
, cast(sum(case when mc.person_id is not null then 1 else 0 end) / count(*) as decimal(18,2)) as percent_members_with_claims
from core.member_months mm 
left join pharmacy_claim mc on mm.person_id = mc.person_id
and
mm.data_source = mc.data_source
and
mm.year_month = mc.year_month
group by mm.data_source
, mm.year_month
order by data_source
,year_month
```
</details>

<details>
  <summary>Members with Pharmacy Claims</summary>

```sql
with pharmacy_claim as (
select 
  data_source
  , person_id
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.pharmacy_claim
GROUP BY data_source
, person_id
)

, members as (
select distinct person_id
,data_source
from core.member_months
)

select mm.data_source
,sum(case when mc.person_id is not null then 1 else 0 end) as members_with_claims
,count(*) as members
,sum(case when mc.person_id is not null then 1 else 0 end) / count(*) as percentage_with_claims
from members mm
left join pharmacy_claim mc on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
group by mm.data_source
```
</details>

<details>
  <summary>Pharmacy Claims with Enrollment</summary>
  
  The inverse of the above. Ideally this number will be 100%, but there could be extenuating reasons why not all claims have a corresponding member with enrollment.

  ```sql
select 
  mc.data_source
  , sum(case when mm.person_id is not null then 1 else 0 end) as claims_with_enrollment
  , count(*) as claims
  , cast(sum(case when mm.person_id is not null then 1 else 0 end) / count(*) as decimal(18,2)) as percentage_claims_with_enrollment
from core.pharmacy_claim mc
left join core.member_months mm on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
and
to_char(mc.paid_date, 'YYYYMM') = mm.year_month
GROUP BY mc.data_source

```
</details>

### Understanding Retail Pharmacy Utilization

<details>
  <summary>Prescribing Providers</summary>

```sql
select 
data_source
,prescribing_provider_npi
,sum(paid_amount) as pharmacy_paid_amount
,sum(days_supply) as pharmacy_days_supply
from core.pharmacy_claim
group by 
data_source
,prescribing_provider_npi
order by pharmacy_paid_amount desc

```
</details>

<details>
  <summary>Pharmacy Names</summary>

```sql
select 
data_source
,dispensing_provider_npi
,sum(paid_amount) as pharmacy_paid_amount
,sum(days_supply) as pharmacy_days_supply
from core.pharmacy_claim
group by dispensing_provider_npi
,data_source
order by pharmacy_paid_amount desc
```
</details>

### Brand vs Generic
<details>
  <summary>Brand Generic Dollar Opportunity</summary>
  
We can view the total dollar opportunity from switching brands to generics with this query.

```sql
select
    data_source
  , sum(generic_available_total_opportunity) as generic_available_total_opportunity
from pharmacy.pharmacy_claim_expanded
group by 
    data_source

```
</details>
<details>
  <summary>Opportunity by Brand Name</summary>
  
To view the drugs that would yield the most savings by switching to generic, we can group by brand name and sort high to low on opportunity.

```sql
select
    data_source
  , brand_name
  , sum(generic_available_total_opportunity) as generic_available_total_opportunity
from pharmacy.pharmacy_claim_expanded
where 
  generic_available_total_opportunity > 0
group by 
    brand_name
  , data_source
order by generic_available_total_opportunity desc

```
</details>
<details>
  <summary>Generic NDCs Available</summary>
  
To view the generic ndcs that exist for a particular brand drug (Concerta in this example), we can join to the generic_available_list table. This will generate one row for every generic that is available, so the generic 'generic_available_for_each_brand_drug' column should not be totalled across each generic.

```sql
select
    e.data_source
  , e.ndc_code as brand_ndc_code
  , e.ndc_description as brand_ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
  , sum(e.generic_available_total_opportunity) as generic_available_for_each_brand_drug
from pharmacy.pharmacy_claim_expanded as e
inner join pharmacy.generic_available_list as g
  on e.generic_available_sk = g.generic_available_sk
where 
  e.brand_name = 'Concerta'
group by 
    e.data_source
  , e.ndc_code
  , e.ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
order by generic_available_for_each_brand_drug desc

```
</details>
<details>
  <summary>Generics Available in Prescribed History</summary>
  
To view only the generics that have been prescribed in the pharmacy claims data history (for a given data source), we can set a filter in the where clause for the generic_prescribed_history flag. This will generate one row for every generic that is available, so the generic 'generic_available_for_each_brand_drug' column should not be totalled across each generic.

```sql
select
    e.data_source
  , e.ndc_code as brand_ndc_code
  , e.ndc_description as brand_ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
  , sum(e.generic_available_total_opportunity) as generic_available_for_each_brand_drug
from pharmacy.pharmacy_claim_expanded as e
inner join pharmacy.generic_available_list as g
  on e.generic_available_sk = g.generic_available_sk
where 
  e.brand_name = 'Concerta'
  and g.generic_prescribed_history = 1
group by 
    e.data_source
  , e.ndc_code
  , e.ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
order by generic_available_total_opportunity desc

```
</details>