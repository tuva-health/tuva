---
id: eligibility
title: "Eligibility"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';

The eligibility table includes information about a patient's health insurance coverage and demographics (note: we use the word patient as a synonym for member).  Every claims dataset should include some sort of eligibility data, otherwise it's impossible to calculate member months, which are needed to calculate measures like PMPM.

Insurance eligibility information is usually formatted in 1 of 2 ways: 

- **Coverage Format:** Every patient has one record per eligibility span (i.e. eligibility start and end date).  The span can be for a single month or for multiple months.
- **Member Months Format:** Every patient has one record for every month of eligibility.  For example, if a patient had 12 months of medical coverage they would have 12 records, one for each month of eligibility.

The eligibility table uses the coverage format because this format is more common in raw claims data.  If your eligibility data is already in this format then you can map it directly (i.e. very little transformation should be needed).  However if your data is in the member months format you will first need to transform it into the coverage format.

<JsonDataTable  jsonPath="nodes.source\.integration_tests\.claims_input.eligibility.columns" />