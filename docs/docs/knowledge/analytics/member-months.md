---
id: member-months
title: "Member Months"
description: This section describes 
---

<iframe width="640" height="400" src="https://www.youtube.com/embed/UNjUwevyBDk?si=8rMBWMlH4g9Ee8rQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

When trending population level statistics such as claims payments or utilization, it's essential to normalize for changes in patient enrollment i.e. eligibility.  The common way to do this is by computing member months and using this as the denominator.  Statistics that have been normalized changes in members months are often reported as "per-member-per-month" or "PMPM".  For example, one would typically look at ED visits PMPM.

In case it isn't obvious, the reason it's a best practice to normalize for changes in enrollment when trending these sorts of statistics is because things like claims payments and utilization will change month-to-month simply because the eligible population changes as members gain/lose eligibility due to changes in employment, birth, death, etc.

The process of calculating PMPM requires assigning claims to a particular member month.  The two date fields most commonly used to do this are claim_start_date and claim_end_date.  Paid_date is less commonly used because doing so will include variation due to claims adjudication e.g. adjustments that occur for some claims over time.  Sometimes this is desired, but for most analyses it's more common to take the date from when the healthcare encounter occurred.

Using claim_start_date will often lead to slightly different results than claim_end_date, though the difference is often small.  Although there is no hard rule, it's more common to use claim_start_date, the thinking being that if a patient loses eligibility during a long encounter, the insurer who covered the patient at the beginning of the encounter is more likely to pay.  However we haven't seen much hard evidence supporting this hypothesis and our current use of claim_start_date is more out of convention than anything else.

## Calculating Member Months

In this section we use an example to describe how to calculate member months.  This is the same methodology we use in the Tuva Project.  

To calculate member months, you need to convert each patient's eligibility record (with start and end dates) into multiple records, with one record for each month of eligibility.  Let's take an example.  Suppose member A1234 has coverage from Aetna from January 1st to June 15th of 2022.  They lose coverage on June 16th and they regain coverage on August 10th.  Further suppose member B2468 has coverage from January 1st through the entire year of 2022.  These two members would have eligibility spans that look like the data below:

| person_id | payer | enrollment_start_date | enrollment_end_date |
| --- | --- | --- | --- |
| A1234 | Aetna | 01-01-2022 | 06-15-2022 |
| A1234 | Aetna | 08-10-2022 | |
| B2468 | Aetna | 01-01-2022 | 12-31-2022 |

Finally, let's suppose the current date is January 31st 2023.

In this example, patient B2468 has an enrollment span with 12 months of continuous eligibility, and so should be counted as having 12 member months.  And it turns out that A1234 should also be counted as having 12 member months, but the assignment isn't as straightforward.  To unpack it, we need to take a brief detour into partial eligibility.

Partial eligibility occurs whenever a patient does not have eligibility for an entire month.  A1234 has partial eligibility for the months of June 2022 and August 2022.  There are multiple methods for handling partial eligibility when computing member months, but the most common method is to assume full eligibility for the entire month.  Not every type of health insurance coverage works like this, but the majority do. In the example above we would give A1234 a full member month for both June 2022 and August 2022, following this method.  

<iframe width="640" height="400" src="https://www.youtube.com/embed/y9toS1ErRXE?si=_cNwH7ANDWWQElip" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen="true"></iframe>

After converting the above enrollment spans to member months (e.g. by using the SQL at the end of this section), the data would look like this:

| person_id | year_month | payer | 
| --- | --- | --- | 
| A1234 | 2022-01 | Aetna | 
| A1234 | 2022-02 | Aetna | 
| A1234 | 2022-03 | Aetna | 
| A1234 | 2022-04 | Aetna | 
| A1234 | 2022-05 | Aetna | 
| A1234 | 2022-06 | Aetna | 
| A1234 | 2022-08 | Aetna | 
| A1234 | 2022-09 | Aetna | 
| A1234 | 2022-10 | Aetna | 
| A1234 | 2022-11 | Aetna | 
| A1234 | 2022-12 | Aetna | 
| A1234 | 2023-01 | Aetna | 
| B2468 | 2022-01 | Aetna | 
| B2468 | 2022-02 | Aetna | 
| B2468 | 2022-03 | Aetna | 
| B2468 | 2022-04 | Aetna | 
| B2468 | 2022-05 | Aetna | 
| B2468 | 2022-06 | Aetna | 
| B2468 | 2022-07 | Aetna | 
| B2468 | 2022-08 | Aetna | 
| B2468 | 2022-09 | Aetna | 
| B2468 | 2022-10 | Aetna | 
| B2468 | 2022-11 | Aetna | 
| B2468 | 2022-12 | Aetna | 

Notice that the last member month given to patient A1234 was for January 2023, since we supposed the present date was January 31st 2023 in our example.

