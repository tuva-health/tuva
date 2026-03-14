---
id: appointment-type
title: "Appointment Type"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

## Data Dictionary

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__appointment_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/appointment_type.csv_0_0_0.csv.gz">Download CSV</a>

## What are appointment type codes?

Value Set of codes that describe the kind of appointment or the reason why an 
appointment has been scheduled. This code set is maintained by 
[HL7 FHIR](https://terminology.hl7.org/6.5.0/ValueSet-v2-0276.html).

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current 
codeset in the Tuva package. Tuva users do not need to complete this step unless 
they are leveraging a different version of codes or are no longer updating to 
the current version of the project, but need an updated terminology set.

1. Navigate to https://terminology.hl7.org/6.5.0/ValueSet-v2-0276.html
2. Copy and paste the code list into any text editor 
3. Keep only the relevant fields, "code" and "description"
4. Format the codes as a CSV file and save 
5. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the public S3 bucket are required)
