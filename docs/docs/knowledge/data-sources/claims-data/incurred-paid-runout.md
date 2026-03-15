---
id: incurred-paid-runout
title: "Claims Lag Issues"
description: This section covers the time lag between the date that healthcare services are performed (i.e. claims are incurred) and when they are processed and paid, and common approaches to accounting for this lag in analytics. 
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 09-23-2025</em></small>
</div>

## Medical Claims Dates

There are typically two important dates to account for when analyzing claims data:

**Incurred Date** This is the date that the healthcare service/s recorded on the claim was rendered. There are tecnically no fields on a claim form labled *incurred date*. Instead this date is typically derived from the Claim Start or Claim End dates on either the claim header or the claim line. Sometimes these dates are supplemented with Admission Start or End dates found on Institutional claims. The Tuva Project for example uses hierarchical assignment logic that first selects the claim header start date, then subsitutes in claim end dates, claim line start & end dates, or admit start and end dates depending on the type of claim. The important thing to know is that this *incurred date* field should represent the date the service was performed or *rendered* (as it is sometimes referred to). 

**Paid Dates** - Paid dates are simply the date that a claim was paid. As with most concepts relating to healthcare though, the actual date that a claim was paid can be subject to interpretation and complexity. Sometimes, claim feeds may use the concepts of *claims processed dates* and *claims paid dates* synonymously, when in fact a claim could be adjudicated/processed one day and an actual claims check or payment processes later. This nuance is often abstracted away unless you are dealing with a claims feed that has both dates, or you work at a health insurance company and must decide which date/concept makes the most sense for your analytical use case. In most analytical cases, the date the claim was processed is sufficient since what we often want to understand is the time lag between when a service was rendered and when we see that reflected in claims. That said, a claims feed from a payer often contains only claims that were actually paid. For the purposes of the rest of this chapter, we will use the term "Paid" date and treat the two concepts synonymously. 

## Claims Lag
Typical claims analytics utilize data feeds that update monthly, with the latest complete month containing all claims that were *paid* in that month. This does not mean however that the latest month contains all claims that were *incurred* in that month. It is common for medical claims to take some time to be submitted by the provider and work their way through the chain of revenue cycle management vendors, clearinghouses, adjudication systems, and internal payer ETL or ELT processes and into the payer data warehouse for analysis or distribution to downstream partners, ACOs, analytics teams etc... 

This delay is commonly referred to in the industry as **Claims Lag**. Claims Lag can significantly impact claims analytics, causing an artificial drop off in cost and utilization in the most recent months preceding the analysis. 


![claims lag image](/img/claims_lag.png)

The figure above shows an example of the impact of claims lag, where the medical claims inpatient visits per 1000 drops off significantly beginning in July. The actual inpatient visits per 1000 is depicted by the "EWS" which in this case represents "Early Warning System" data that tracks inpatient utilization via non-claims sources like Hospital Admissions or EMR data. The delta between the two is shaded in blue. As you can see, utilization is dramatically underestimated in later months. This lag represents a typical, predictable, phenomenon in healthcare economics that is typically dealt with in 2 key ways: 

## Claims Runout

Wait for sufficient time to elapse so that lagging claims show up in the data and you have a more complete picture. Typically, this involves waiting until at least three full months have elapsed since the last incurred date in the period of claims you are analyzing. At this point, it is typical—but not guaranteed—to have nearly complete claims, if you are receiving the updated claims data feeds very soon after processing (typical in health plans). If you are receiving claims as a provider or third party, sometimes you will require more lag time to account for delays in when you get your extract. You should typically receive the incurred and paid time periods covered by the extracts you are receiving. Oftentimes, value-based care contracts specify how much paid runout is required to conduct the final analysis of the claims, and it is often at least six months of paid claims runout from the last month of incurred claims (e.g., Jan 2024–Dec 2024 incurred, Jan 2024–June 2025 paid).
## Completion Factors

 A set of adjustment factors or weights that are applied to aggregate costs for each month, such that the costs are adjusted to account for lagging claims. The impact of these adjustments are greater in more in recent months. These factors are derived from historical analysis of the completeness of claims relative to a given point in time. The output of this analysis is commonly referred to as a *lag traingle*, which plots the percentage of incurred claims that have been paid in each month since the incurred month. This is done using historical data so the percentage of claims is truly known. 
 
 In the table below, you can see that in past years, 60% of claims incurred in January were paid in January, 85% of claims incurred in January were paid by the end of February (1 month following incurred date), 93% of claims incurred in January were paid in March, and so on. As you can see in this example, it is possible for some claims to lag for very long periods of time (note that in some cases even 12 months out from the incurred date, the completion rate is less than 100%) This is often due to pending manual review or disputes between providers and payers that result in delays in processing and payment of claims. Often, these lagging claims are higher cost claims, typically those reflecting inpatient admissions. For this reason, completion factors are sometimes calculated separately for different service categories (eg. Inpatient, Outpatient, Professional, Pharmacy, Other). 

<div style={{ textAlign: "center", marginBottom: "-2.75rem" }}>
  <strong>Age of Claim in Months Since Incurred Date</strong>
</div>
| Incurred Month              | 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    | 10   | 11   | 12   |
|-----------------------------|------|------|------|------|------|------|------|------|------|------|------|------|------|
| Jan                         | 60%  | 85%  | 93%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.2%| 99.5%| 99.7%| 99.8%| 100% |
| Feb                         | 58%  | 84%  | 92%  | 95%  | 97%  | 98%  | 98.5%| 99%  | 99.2%| 99.5%| 99.7%| 99.8%| 100% |
| Mar                         | 59%  | 83%  | 91%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| Apr                         | 61%  | 85%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| May                         | 60%  | 84%  | 91%  | 94%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| Jun                         | 62%  | 85%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| Jul                         | 61%  | 84%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| Aug                         | 60%  | 85%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| Sep                         | 59%  | 84%  | 91%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.7%| 99.8% |
| Oct                         | 60%  | 85%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%|   –  |   –  |
| Nov                         | 61%  | 84%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%|   –  |   –  |   –  |   –  |   –  |
| Dec                         | 60%  | 85%  | 92%  | 95%  | 96%  | 97%  |   –  |   –  |   –  |   –  |   –  |   –  |   –  |
| **Completion Factor (Avg.)**| 60%  | 84%  | 92%  | 95%  | 96%  | 97%  | 98%  | 98.5%| 99%  | 99.3%| 99.5%| 99.8%| 100% |

 The percentage of completeness in each column is averaged across all incurred months evaluated in the time period being analyzed to produce a *completion factor* as shown in the bottom row of the table. You can then *complete* claims by dividing the aggregate cost for each month by the completion factor for the age of that claim to get a projected cost. See the example below to see how this works in practice. 

### Completion Factor Example: Analyzing a full year of incurred claims using completion factors (All data is illustrative only)

**Incurred Dates:** January–December 2024
**Paid Dates:** January-December 2024

In this example, we are analyzing claims in January 2025 and want to make some estimates of claims completeness even though we don't yet have sufficient claims runout to consider the period complete.
To do this, we will take the total cost aggregated for each incurred month and divide by the completion rate that corresponds to its **current age** (months since incurred date through December).  
Formula: `Projected = Observed / Completion Factor`.

| Incurred | Current Age | Unadjusted Cost        | Completion Factor | Completed Cost        |
|:---------|------------:|-----------------------:|------------------:|----------------------:|
| Jan      | 11          | $1,000,000             | 100.0%            | $1,000,000            |
| Feb      | 10          | $1,000,000             | 100.0%            | $1,000,000            |
| Mar      | 9           | $1,000,000             | 100.0%            | $1,000,000            |
| Apr      | 8           | $1,000,000             | 100.0%            | $1,000,000            |
| May      | 7           | $1,000,000             | 100.0%            | $1,000,000            |
| Jun      | 6           | $1,000,000             | 100.0%            | $1,000,000            |
| Jul      | 5           |   $996,000             | 99.6%             | $1,000,000            |
| Aug      | 4           |   $990,000             | 99.0%             | $1,000,000            |
| Sep      | 3           |   $960,000             | 96.0%             | $1,000,000            |
| Oct      | 2           |   $930,000             | 93.0%             | $1,000,000            |
| Nov      | 1           |   $850,000             | 85.0%             | $1,000,000            |
| Dec      | 0           |   $600,000             | 60.0%             | $1,000,000            |
| **Total**|             | **$11,326,000**        |                   | **$12,000,000**       |

- **Unadjusted Cost** (what’s actually paid through Dec): **$11.326M**  
- **Completed Cost** (what's projected after completion factors applied): **$12.000M**  


