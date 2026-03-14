---
id: tuva-provider-attribution
title: "Tuva Provider Attribution"
---
import AttributionSampleDashboard from '@site/src/components/AttributionSampleDashboard';

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/provider_attribution)

Provider attribution assigns each person to the provider who most plausibly
manages their primary care. Organizations use attribution for panel
management, quality and cost performance measurement, contracting,
network optimization, and outreach workflows.

The Tuva Provider Attribution mart implements a transparent, CMS-style
primary-care attribution that runs using only Tuva’s standard inputs
(claims mapped to the Tuva Input Layer, the Core `member_months`, and
Tuva terminology/reference data) - no extra inputs are required.

The logic is inspired by CMS attribution used in ACO/REACH contexts
but generalized to work across all payers and claim types brought into Tuva.
To improve coverage and analytics usability, we add two
additional fallback passes (Steps 4–5) that expand the window and relax
provider classification requirements only after the earlier CMS-like
passes do not yield an assignment.

Specifically, the mart:

- Classifies rendering NPIs as PCP, Specialist, or NPP using NPPES and the Medicare taxonomy crosswalk.
- Identifies primary-care HCPCS services from claims.
- Requires eligibility via member months within the lookback window.
- Applies a five-pass methodology to find the best-fitting provider, adding
  broader fallback behavior only when needed.

### Five-Pass Methodology

For each person, we evaluate providers in ordered passes and select the
first pass that yields any qualifying provider(s). Within the chosen pass,
providers are ranked by highest allowed_amount with a member (fallback to paid_amounts if allowed isn't provided), and then highest number of visits.

1. 12-month PCP/NPP primary-care HCPCS
2. 12-month Specialist primary-care HCPCS (only if Step 1 has no result)
3. 24-month PCP/NPP primary-care HCPCS
4. 24-month Primary-care HCPCS (any provider classification)
5. 24-month Any rendering NPI (fallback when HCPCS-based classification fails)

Windows differ slightly by output:

- Current: The last 12 or 24 calendar months ending on `as_of_date`. See [Current Output Date Behavior and Configuration](#current-output-date-behavior-and-configuration) for defaults and overrides.
- Yearly: Calendar-year windows (Jan..Dec) for the performance year, with expanded windows spanning Jan of Y-1 through Dec of Y (24 months) as a fallback.

The ranking table exposes all qualifying providers and the first pass each
qualifies for. The assignment tables choose the top-ranked provider (rank = 1)
or emit a labeled fallback when no assignable history exists.

### Inputs and Dependencies

- Claims: `core.medical_claim`
- Eligibility: `core.member_months`
- Terminology and value sets:
  - `cms_provider_attribution__primary_care_hcpcs_codes`
  - `cms_provider_attribution__provider_specialty_assignment_codes`
  - `reference_data.calendar`
  - `terminology.provider` (NPPES and taxonomy crosswalk curated by Tuva)

### Current Output Date Behavior and Configuration

The “current” scope uses a data-driven `as_of_date` to define its rolling
12- and 24-month windows:

- Default: the maximum claim_end_date in the attribution claim set, if it is
  not null and is not a future date; otherwise the system date at runtime.
- Override: set the dbt var `provider_attribution_as_of_date` to a `YYYY-MM-DD`
  value to pin `as_of_date`.

Examples:

Pin to a date:

```bash
dbt build --select tag:tuva_provider_attribution \
  --vars '{"provider_attribution_as_of_date":"2025-10-01", "claims_enabled": true}'
```

Use defaults (no var) and enable via global Tuva flags:

```bash
dbt build --select tag:tuva_provider_attribution \
  --vars '{"claims_enabled": true}'
```

Notes:
- Models are enabled when `tuva_provider_attribution` (or `claims_enabled` or
  `tuva_marts_enabled`) evaluates true.
- The “current” output runs for every person with at least one member month
  in the last 12 months ending at `as_of_date`. Persons without assignable
  history receive a labeled fallback row to keep the output grain of the tables at one row for every member with eligibility during the evaluation period.

## Example SQL

<details>
  <summary>Count Assigned by Step (Current)</summary>

```sql
select
  assigned_step
, count(*) as members
from tuva_provider_attribution.assigned_beneficiaries_current
group by
  assigned_step
order by
  assigned_step;
```
</details>

<details>
  <summary>Provider Panel Size and Context (Current)</summary>

```sql
select
  provider_id
, provider_bucket
, count(*) as attributed_members
, sum(visits) as visits
, cast(sum(allowed_amount) as decimal(18,2)) as allowed_amount
from tuva_provider_attribution.assigned_beneficiaries_current
where provider_bucket <> 'no_eligible_history'
group by
  provider_id
, provider_bucket
order by
  attributed_members desc;
```
</details>

<details>
  <summary>Fallback Rate (Current)</summary>

```sql
select
  cast(sum(case when provider_bucket = 'no_eligible_history' then 1 else 0 end) as decimal(18,2))
    / nullif(count(*), 0) as fallback_rate
from tuva_provider_attribution.assigned_beneficiaries_current
```
</details>

<details>
  <summary>Members With No Assignable History (Current)</summary>

```sql
select
  person_id
, as_of_date
from tuva_provider_attribution.assigned_beneficiaries_current
where provider_bucket = 'no_eligible_history';
```
</details>

<details>
  <summary>Comparing Yearly Attribution</summary>

```sql
with a as (
  select
    person_id
  , provider_id
  from tuva_provider_attribution.assigned_beneficiaries_current
  where as_of_date = date '2024-11-30'
    and provider_bucket <> 'no_eligible_history'
),
b as (
  select
    person_id
  , provider_id
  from tuva_provider_attribution.assigned_beneficiaries_current
  where as_of_date = date '2024-12-31'
    and provider_bucket <> 'no_eligible_history'
)
select
  coalesce(a.provider_id, 'none_prior') as prior_provider
, coalesce(b.provider_id, 'none_current') as current_provider
, count(*) as members
from a
full outer join b using (person_id)
where coalesce(a.provider_id, 'none') <> coalesce(b.provider_id, 'none')
order by
  members desc;
```
</details>

<details>
  <summary>Top-3 Ranked Providers Per Person</summary>

This surfaces why a specific provider was chosen by comparing step,
allowed_amount, and visits across candidates.

```sql
with pr as (
  select
    person_id
  , scope
  , performance_year
  , as_of_date
  , lookback_start_date
  , lookback_end_date
  , provider_id
  , provider_bucket
  , step as earliest_step
  , step_description
  , allowed_amount
  , visits
  , ranking
  from tuva_provider_attribution.provider_ranking
  where scope in ('current','yearly')
)
select
  person_id
, scope
, performance_year
, as_of_date
, lookback_start_date
, lookback_end_date
, provider_id
, provider_bucket
, earliest_step
, step_description
, allowed_amount
, visits
, ranking
from pr
where ranking <= 3
order by
  person_id
, scope
, ranking;
```
</details>

## Sample Dashboard

Below is an embedded, interactive sample built from small CSVs bundled with the docs. Select a measurement period to see coverage, step mix, and top providers.

<AttributionSampleDashboard 
  currentCsvUrl="/data/tuva_provider_attribution/assigned_beneficiaries_current_sample.csv"
  yearlyCsvUrl="/data/tuva_provider_attribution/assigned_beneficiaries_yearly_sample.csv"
  rankingCsvUrl="/data/tuva_provider_attribution/provider_ranking_sample.csv"
/> 
