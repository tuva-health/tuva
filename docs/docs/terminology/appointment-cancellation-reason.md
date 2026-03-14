---
id: appointment-cancellation-reason
title: "Appointment Cancellation Reason"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

## Data Dictionary

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__appointment_cancellation_reason.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/appointment_cancellation_reason.csv_0_0_0.csv.gz">Download CSV</a>

## What are appointment cancellation codes?

The coded reason for the appointment being canceled. This is often used in 
reporting/billing/further processing to determine if further actions are 
required, or specific fees apply. This code set is maintained by [HL7 FHIR](https://build.fhir.org/valueset-appointment-cancellation-reason.html).

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current 
codeset in the Tuva package. Tuva users do not need to complete this step unless 
they are leveraging a different version of codes or are no longer updating to 
the current version of the project, but need an updated terminology set.

1. Navigate to https://build.fhir.org/valueset-appointment-cancellation-reason.html
2. Copy and paste the code list into any text editor 
3. Keep only the relevant fields, "code" and "description"
4. Format the codes as a CSV file and save 
5. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the public S3 bucket are required)
