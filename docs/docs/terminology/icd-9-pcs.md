---
id: icd-9-pcs
title: "ICD-9-PCS"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 05-21-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_9_pcs.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_9_pcs.csv_0_0_0.csv.gz">Download CSV</a>

## What is ICD-9-PCS?

**ICD-9-PCS** stands for *International Classification of Diseases, 9th Revision, Procedure Coding System*. It was used in the United States to code **inpatient hospital procedures** and was part of the broader **ICD-9-CM** (Clinical Modification) system.

- **Maintained by**: Centers for Medicare & Medicaid Services (CMS)
- **Purpose**: Provided a standardized coding system for tracking and billing inpatient procedures.
- **Usage**: Required for reporting procedures on Medicare and other hospital inpatient claims before the adoption of ICD-10.

## Relationship to ICD-9-CM

ICD-9-CM had three volumes:
- **Volume 1**: Tabular list of diagnoses
- **Volume 2**: Alphabetic index to diseases
- **Volume 3**: Procedure codes and descriptions (i.e., **ICD-9-PCS**)

Thus, ICD-9-PCS was **included within ICD-9-CM** as Volume 3. It did not exist as a standalone system but was integrated into the ICD-9-CM framework.

### How this differs from ICD-10:

With the transition to ICD-10 in 2015, the coding systems were **split into two distinct and separately maintained systems**:
- **ICD-10-CM**: For diagnoses (maintained by NCHS)
- **ICD-10-PCS**: For inpatient procedures (maintained by CMS)

This separation allowed for more targeted updates and specialization in how diagnoses and procedures are represented, in contrast to the integrated structure of ICD-9-CM.

## When and Why was ICD-9-PCS Retired?

ICD-9-PCS was retired on **October 1, 2015**, alongside the rest of ICD-9-CM.

### Reasons for retirement:
- **Limited capacity**: Its numeric-only format and hierarchical constraints limited specificity.
- **Inability to reflect modern procedures**: Advances in technology and surgical methods could not be adequately coded.
- **ICD-10-PCS**: Introduced a new alphanumeric structure with greater detail, flexibility, and logic, designed from the ground up to support modern healthcare analytics and billing.

## Code Structure

ICD-9-PCS codes:
- Were **2 to 4 digits**, numeric only
- Represented procedures performed during inpatient hospital stays
- Example: `81.54` — Total knee replacement

### Limitations:
- Codes were not always intuitive or comprehensive
- Lacked the multi-axial structure used in ICD-10-PCS (which uses 7 characters with defined positions for section, body system, root operation, body part, approach, device, and qualifier)

> Note: Legacy data coded using ICD-9-PCS remains valuable for trend analyses, quality measurement, and studies involving historical hospitalization data.


## Maintenance Instructions

The ICD-9-PCS coding system was officially retired in the United States on October 1, 2015. As such, it is no longer subject to updates or revisions. The currently available version represents the final and most recent release of this code set.
