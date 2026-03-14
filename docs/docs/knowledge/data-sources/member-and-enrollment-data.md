---
id: member-and-enrollment-data
title: "Eligibility and Enrollment Data"
---

## Eligibility Overview

Eligibility and enrollment data is a critical piece of data tracked by health insurance companies that determines when a person (i.e. member) was eligible to receive specific health benefits.  Note that "enrollment" and "eligibility" are often used interchangeably and typically refer to the same type of data; however, if a partner is providing two kinds of files, the eligibility files are generally more bare-bones and include very little demographic information, focusing exclusively on actual eligibility spans (i.e. start dates and end dates). Enrollment files generally include broader demographic and contact information for the member. 

When you begin supporting a member population, one of the first things you'll need to determine is "who is eligible for this program?" This may initially seem straightforward, but there's generally a lot of nuance to how eligibility checks and transmission of eligibility information are shared between organizations. 

## Common Methods of Eligibility Transmission

1. **Eligibility APIs**: Eligibility APIs are often available and required to validate the eligibility of the member/patient for services at the time of enrollment or service. Note that these eligibility checks are usually real-time (or allow you to check eligibility on a particular date), and only tell you whether a member is covered at that moment in time. Eligibility APIs are not generally good at determining the entire history of coverage for a member, and this is why they are typically paired with eligibility files that provide a historical record of a member's coverage.

2. **Eligibility Flat Files**: Eligibility files in a "flat" format (CSV, pipe-delimited, fixed-width, etc.) continue to be the industry workhorse. Most employers, brokers, and health plans rely on 834s or proprietary formats to exchange large batches of enrollment data on a set schedule (generally weekly or monthly). These files are dependable for exchanging large volumes of data, but they have a lag — retro changes or new enrollments may not be reflected until the next file cycle.

Given the strengths and weaknesses of APIs and flat files, most organizations use the two side by side. Flat files handle the bulk transfers, while APIs provide a way to layer on more timely updates and reconcile discrepancies. For example:
- A flat file may load an employer's full membership each month.
- APIs can be used daily to check for new enrollments, terminations, or changes in coverage tiers.

Tuva relies exclusively on data received from flat files, as they are the best suited for analytical use cases and allow historical data to be altered and re-run (more on this later).  However, the above context should help in understanding discussions between organizations around eligibility verification.  The remainder of this section will focus on eligibility file formats and how they should be leveraged to ensure accurate analytics.

## Common Eligibility File Formats

- **Eligibility Spans**: Eligibility files that leverage eligibility "spans" have only a single record per patient for a particular contiguous timeframe of eligibility (there's some nuance here if other member identifiers change, but we'll cover that more later on). Essentially, the number of records in the file will be substantially less, as members are not repeated as frequently in the "member month" file format outlined below. 

- **Member Months**: Member month files have a single record for each member, for each month, for each type of coverage they have (coverage type is only relevant if multiple coverage types are applicable, for instance, separate medical, dental, and vision coverage). Eligibility and demographic information can change in each individual member's monthly record (more on this in the Attributes section below). Generally, the latest information received from a member for that particular month is included in the monthly record, and any member's single-day coverage for a given day is included in that month.

## Retroactivity
Healthcare organizations rarely deal with data that remains static. "Retroactive" changes (often just referred to as "retroactivity") occur frequently due to changes in coverage that require a particular member's coverage to be backdated in some way. Common reasons for retroactivity changes are:
- A newborn added after birth
- An employee who backdates coverage after a job change
- A member's coverage is backdated due to non-payment (after a grace period)
- A dependent dropped months after a divorce.

These retroactive changes complicate the tracking of eligibility and can also have downstream impacts in other areas of healthcare data. Claims that were already processed may need to be reversed or reprocessed. Membership counts can swing up or down long after the fact, and billing and payroll systems may not align with the updated coverage. Each adjustment creates opportunities for mismatches between systems, making reconciliation a perennial task that every healthcare organization must manage. There is no "magic bullet" to manage retroactive changes; the best policy is to ensure you align on how to manage retroactive changes internally for reporting and with organizations you are transmitting data to and from.

## Common Eligibility Types

**Incremental**: Incremental eligibility files are generally used when providing a member month file format, with only the most recent member months being provided in each individual file. This is the simplest method of transmission logic, as you can combine each individual file to obtain a complete history of eligibility; however, it will not capture retroactive changes to eligibility that may have occurred. For administering many programs, capturing retroactivity is unnecessary; however, it will impact many analytic calculations and, therefore, can cause issues when reconciling data across multiple programs.

**Full Replacement**: Full replacement eligibility files provide a complete history of the eligibility records since the inception of the contract/program. Full replacement files can be supplied in either eligibility span or member month format, and allow retroactive changes to be captured correctly, and are the most accurate for reconciling changes over time; however, given that the files will continue to grow over time, this can cause some performance issues for transmission and processing over time.

**Hybrid**: The limitations of incremental and full replacement files sometimes lead organizations to provide a hybrid set of files. Incremental files are leveraged for regular weekly/monthly updates, but yearly "true-up" files are provided with the "final" eligibility data for past years of data (generally after a period of run-out, 3 months being the most common). The final eligibility files are used for historical data, while incremental files are used where final data is not yet available, providing a "best of both worlds" approach that can be helpful for many programs.

## Eligibility File Attributes

**Start & End Dates**:
    - Eligibility start and end dates represent the beginning and end of a particular coverage. However, note that the existance of start or end date does NOT necessarily mean a member is new or is terminating coverage, a member may have changes plans, moved, or had some other attribute change that requires eligibility to change, and you will need to look across all eligibility records to determine if a member is truly new, termed, or just had a change in eligibility requiring a reset of start/end dates.
    - Eligibility start and end dates are most commonly the first day and last day of a month, but note that this is not always the case. Sometimes mid-month start/end dates are required, this could be because of an employment event (mid-month hiring), a life event (birth of a child), or a program transition (Medicare/Medicaid transitions are often on the exact eligibility determination date)

**Subscriber/Cardholder**:
    - Identifying the primary subscriber, contract holder, or cardholder is often necessary for understanding plan coverage and identifying/differentiating which members are covered under a particular policy. Typically, an eligibility file will have both a member ID and a subscriber ID; the member ID is unique to each individual member. In contrast, the subscriber ID is repeated for all members covered by the policy, which can be used to "group" members that are covered together. In addition to the subscriber ID used for grouping members, the members' "relationship" to the subscriber may also be listed; common relationship types used are:
        - Spouse / Partner
        - Child / Dependent
        - Covered Adult

**Demographics**:
    - Eligibility files generally also include member demographics and contact information; however, sometimes this information is provided separately in an "enrollment" or "marketing" file. If this is the case, you need to ensure that common member identifiers are available to combine the files and that you have effective dates in both files to make joining them seamless
    - Note that changes in demographic information are often the cause of eligibility start/end date changes, so you may see eligibility spans that look unnecessary based on the plan coverage, but are due to demographic changes like a change of address.
    - Age/Birth Date: Age/birth date is also commonly used; however, there is nuance here that must be accounted for. When calculating a person's age, it's essential to determine whether the file is based on the age as of the date of file generation or the date of coverage for that particular record. Best practice is to calculate the member's age for the timeframe in question so that analytics are accurate when you are analyzing data based on the age of a member (past year's data reflecting the member's current age will mislead and cause issues)
    - Other Common Attributes:
        - Plan / Group Info: 
        - Address, Email, Phone Numbers:
        - Other program eligibility (DUAL-coverage, subsidies eligibility, etc.)

**Provider Alignment/PCP Election**: Often programs need to understand which provider a member is associated with in order to track performance or understand which provider gets to "take credit" for a member over a given time period. This is especially important under value based care and other financial agreements, but is also useful when understanding who is in the best position to help manage a particular patient. A variety of provider relationships are sometimes included in an eligibilty file:
        - PCP Election: A member has proactively selected a physician as their PCP
        - Provider Attribution: A member has been determined to be aligned to a physician based on their claims history (i.e. they see that provider most frequently)
        - Provider Alignment: A member may be "assigned" to a particular physician, provider group, or health system based on contractual or other program specifications

## EHR Coverage Data
Coverage data that exists EHRs differs slightly from the information received on a traditional eligibility file, as its purpose is not to validate coverage but rather to store the most up-to-date information for that member. Many EMRs will have a history of changes in address and other demographic information for a member; however, the EHR patient data is often a snapshot in time, capturing only the latest demographic and health plan coverage information for a patient. You will need to account for this when creating historical analytics for a particular patient, and it may require combining the EHR data with other eligibility information (for instance, health plan eligibility files) to determine the history of coverage for a specific plan or program.

## Sources of Enrollment Data
- Payers
- Clearinghouses/Aggregators
- CMS
- NAIC

## Connecting Eligibility to Member Months
No matter how eligibility and enrollment data is received, the data will need to be transformed into a "member month" format, which is the lingua franca of healthcare analytics and a corner stone to many of the different healthcare measures like PMPM, Util/1000 and generally understanding the changing size of a population over time. One of the most common transformations that occurs in healthcare analytics is to transform eligibilty spans into member months, and this is a part of the Tuva Project core transformations as well. 

Here are some rules of thumb for how much coverage is required for a member to be included in a particular member month:
- Frequent rules of thumb are that a member is included in a member month if they have any coverage during the month in question (even a single day), they must have coverage on a particular day (i.e. the 15th of the month), or they must have a full month's coverage to be included. Each methodology has it's pluses/minuses, the most important thing is that consistent rules are used and that stakeholders understand the criteria.
- Often a member will be included in the member month table if they have any coverage, but separate "count" fields are included to allow stakeholders to count only members active on the 1st, 15th, last day, or full months coverage for different downstream use cases, this provides the most flexibility but can lead to inconsistencies in analysis if their is not alignment on which particular definition to use in different scenarios.

See [here](../../knowledge/analytics/member-months.md) for a more complete description of member months.