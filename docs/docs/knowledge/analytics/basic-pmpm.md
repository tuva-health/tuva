---
id: basic-pmpm
title: "Cost Metrics (PMPM)"
---

# Understanding PMPM in Healthcare

## What is PMPM?
**PMPM** stands for **Per Member Per Month**.  
It is a common financial and utilization metric used in healthcare to normalize costs across a population over time. By standardizing results to a monthly, per-member basis, PMPM allows organizations to compare performance across groups of different sizes, risk profiles or time periods.

---

## Why PMPM is Important
- **Standardization**: Adjusts for differences in population size and enrollment duration.
- **Comparability**: Enables consistent comparisons across health plans, providers, and programs.
- **Budgeting & Forecasting**: Helps payers and providers model expected costs and monitor financial performance.
- **Performance Measurement**: Used in contracts, quality programs, and benchmarking to evaluate efficiency and value.

---

## How PMPM is Calculated
The general formula is:

**PMPM = Total Cost / Member Months**

Where:
- **Total Cost** = the dollar value of interest (e.g., total paid claims, allowed charges, pharmacy spend, etc.)
- **Member Months** = the denominator, calculated as the sum of all enrolled members across all months in the measurement period. See [member month section](docs/knowledge/analytics/member-months.md) for more details. 

For example:
- If 1,000 members are enrolled for the entire year, that equals **12,000 member months**.
- If the total spend is **$10,000,000**, then: 

PMPM = $10,000,000 / 12,000 = $833


## 2 ways to think about PMPM

### Contributive PMPM

In most medical economic or actuarial analysis when the objective is to understand the drivers of healthcare cost in a given population, PMPM cost is summarized at a population level, and then broken down across different dimensions such as service categories or conditions to understand their contribution to the total PMPM. In this type of *contributive* PMPM, values are ALWAYS divided by the total population member months so that PMPM for all sub-groups sum to the total PMPM.

Taking our 1,000 member population enrolled for the entire year and total spend of $10,000,000 again as the example:

- Total PMPM = $10,000,000 / 12,000 = $833
- Inpatient PMPM = $4,000,000 / 12,000 = $333
- Office-based PMPM = $2,500,000 / 12,000 = $208
- Outpatient PMPM = $2,500,000 / 12,000 = $208
- Ancillary PMPM = $500,000 / 12,000 = $42
- Other PMPM = $500,000 / 12,000 = $42
- The sum of each service category PMPM equals the total PMPM: $333 + $208 + $208 + $42 + $42 = $833

### Relative PMPM

In analytics comparative or disease specific analytics, it is more common to calculate PMPM values at each level by dividing by the member months for that sub-group so that PMPM values are comparable across populations. For example, if you had a population of 12,000 members where 500 members had heart disease, and you wanted to compare their average per member per month cost with that of other populations, you would sum the total heart disease associated costs and divide by the member months for the 500 members with heart disease. This type of PMPM is *relative* to the sub-population of people with heart disease and can be accurately compared to other relative PMPM costs of other cohorts of people with heart disease. 
