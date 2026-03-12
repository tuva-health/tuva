---
id: acute-inpatient-analytics
title: "Acute Inpatient Data Mart"
description: This guide describes how to analyze acute inpatient hospital visits using claims data.
toc_max_heading_level: 2
---

This guide describes the acute inpatient data mart, including how it works and how to use it.  The data mart and guide are still under active development but everything released thus far is fully functioning.

You can find the code for the data mart on GitHub here.

We welcome questions and suggestions for improvement in Slack or GitHub.

## Overview

A significant portion of overall healthcare services and expenditure occur in the acute inpatient hospital setting.  As a result, analyzing how care is delivered during an acute inpatient stay and post-discharge are some of the most common and important analytic uses of claims data.

Hospitals are commonly concerned with optimizing how care is delivered _during_ a hospital stay and the outcomes that result.  This sort of "within hospital stay" analysis typically boils down to figuring out which DRGs the hospital can improve on when it comes to mortality, length of stay, and cost.  Thanks to [CMS's Hospital Readmissions Reduction Program](https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/hospital-readmissions-reduction-program-hrrp), which results in penalties for hospitals with high risk-adjusted readmission rates, hospitals also pay careful attention to readmissions.

Health plans and value-based care organizations tend to be more interested in what occurs _after_ a hospital stay (i.e. post-discharge).  While they have less of an ability to impact what occurs during a stay, they can impact transitions of care post-discharge, which can lead to better patient outcomes, reduced readmissions, and ultimately a lower total cost of care.

With both within-hospital and post-discharge types of analysis, it's critical to risk-adjust the measures of interest.  Otherwise it's impossible to accurately analyze differences across cohorts or changes over time.

But before any analysis can be done, a signficant amount of upfront data transformation must be performed.  This includes:

- Identifying claims that occur in an acute inpatient care setting
- Grouping acute inpatient claims into encounters (i.e. distinct visits)
- Identifying and adjusting for data quality problems
- Applying risk adjustment models

This guide describes how we perform this data transformation in the Acute Inpatient data mart.  It also describes how you can use the data mart to analyze the types of questions outlined above.

## Identifying Acute Inpatient Claims

Identifying which claims occurred in an acute inpatient care setting is the very step in going from raw claims data to data tables ready for acute inpatient analytics.  These are the claims we will group into distinct acute inpatient encounters, but to do that we must first identify them.  

We define an acute inpatient visit as a short-term hospital stay in a standard acute care hospital (e.g. community, tertiary, academic, critical access).  We do not consider visits to psychiatric hospitals, inpatient rehab centers, or nursing facilities acute inpatient stays.

### Institutional Claims

Every acute inpatient encounter occurs in a hospital and every hospital bills the patient's primary health insurance using an institutional claim form.  Therefore we use acute inpatient institutional claims as the foundation for our definition of acute inpatient encounters.

To identify whether an institutional claim occurred in an acute inpatient setting we consider three criteria.  Namely, whether the claim had:

1. Room and Board Revenue Codes
2. Valid MS-DRG or APR-DRG
3. Inpatient Bill Type Code

The diagram below describes the dbt models from the acute inpatient data mart which are used to define which institutional claims are acute inpatient.  Reviewing this diagram in detail is the best way to completely understand how this part of the data mart works.

<iframe width="768" height="432" src="https://miro.com/app/live-embed/uXjVN-Gw0Bw=/?moveToViewport=-4642,-3174,7288,5093&embedId=549531298059" frameborder="0" scrolling="no" allow="fullscreen; clipboard-read; clipboard-write" allowfullscreen></iframe>

The most interesting model in this part of the data mart is the **aip_venn_diagram_summary** model.  This model summarizes the distinct count of institutional claims that meet different combinations of the criteria listed above.  

![AIP Venn Diagram](/img/aip_venn_diagram.png)

For example "rb" indicates claims that have a valid room and board revenue code, but do not have a valid DRG or inpatient bill type code.  And "drg" indicates claims that have a valid DRG but no room and board revenue code or inpatient bill type.  And so on for each combination of the criteria.  There are 7 combinations in total (2^3 = 8 however we ignore the empty set, therefore 7).

Claims datasets vary significantly in data quality.  You can query this table to figure out which combinations of criteria your claims meet based on its own data quality.

![AIP Venn Diagram Example](/img/aip_venn_diagram_example.png)

These results, which are from the Tuva Synthetic dataset, show that there are 223 claims that meet all 3 criteria.  However there are 215 claims with a valid DRG and inpatient bill type that do not have a valid room and board revenue code.  If we require all three criteria in our definition, these 215 claims will not be considered acute inpatient, even though they have a valid DRG.  

The point is, you can use this information the determine the best way to define acute inpatient institutional claims for your specific dataset.  We use all 3 criteria by default in the acute inpatient data mart.  However, you can easily modify the logic in the **acute_inpatient_institutional_claims** model to suit your data.  The logic that you would modify exists in the second CTE in this model as shown in the image below.  For example, to change from using all three criteria to just DRG and inpatient bill type criteria you would change the WHERE statement to: ```where aa.drg_bill = 1```

![Modify AIP Logic](/img/modify_aip_logic_example.png)

#### Room and Board Revenue Codes

As you get deeper into the weeds of your claims data it can be helpful to develop more intuition about room and board revenue codes.

There are two other models that provide interesting data quality analytics related to room and board revenue codes:
- **types_of_room_and_board_rev_codes_on_claims**
- **distinct_room_and_board_rev_codes_per_claim**

In the acute inpatient data mart, we group room and board revenue codes into four categories:
- Basic
- Hospice
- Leave of Absence
- Behavioral

Only basic room and board codes are considered acute inpatient.  However it's possible for institutional claims to have different combinations of different types of room and board codes.  The **types_of_room_and_board_rev_codes_on_claims** model summarizes this.  For example, as the fourth row in the table in the image below shows, there are 11 claims that only have Hospice room and board codes.  These 11 claims will not be considered acute inpatient.  

![AIP Types of Room and Board Codes](/img/aip_types_of_room_and_board_codes.png)

## References

- [Administrative Healthcare Data](https://www.amazon.com/Administrative-Healthcare-Data-Content-Application/dp/1612908861)
- [The Impact of Standardizing the Definition of Visits on the Consistency of Multi-Database Observational Health Research](https://bmcmedresmethodol.biomedcentral.com/articles/10.1186/s12874-015-0001-6)
- [Methodology for Identifying Inpatient Admission Events](https://medinsight.com/healthcare-data-analytics-resources/blog/methodology-for-identifying-inpatient-admission-events/)