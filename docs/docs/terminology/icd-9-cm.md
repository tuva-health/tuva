---
id: icd-9-cm
title: "ICD-9-CM"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 05-21-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_9_cm.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_9_cm.csv_0_0_0.csv.gz">Download CSV</a>

## What is ICD-9-CM?

**ICD-9-CM** stands for *International Classification of Diseases, 9th Revision, Clinical Modification*. It was the U.S. clinical adaptation of the **ICD-9** system originally developed by the **World Health Organization (WHO)**. The “CM” modification added more detail to support diagnostic coding for billing and healthcare analytics in the United States.

- **Maintained by**: The National Center for Health Statistics (NCHS) for diagnoses, and the Centers for Medicare & Medicaid Services (CMS) for procedures.
- **Purpose**: Standardized coding system for documenting diagnoses and inpatient hospital procedures.
- **Usage**: Widely used for medical billing, health statistics, reimbursement, and epidemiological research from the 1980s until its retirement in 2015.

## When and Why was ICD-9-CM Retired?

ICD-9-CM was officially retired on **October 1, 2015**, and replaced by **ICD-10-CM** (for diagnoses) and **ICD-10-PCS** (for inpatient procedures) in the United States.

### Reasons for retirement:
- **Limited capacity**: The ICD-9-CM code system lacked the granularity needed to support modern clinical practice and healthcare data analysis.
- **Outdated structure**: It could not easily accommodate new medical knowledge, emerging diseases, or evolving technology.
- **Improved specificity**: ICD-10-CM/PCS offers significantly more codes, better capturing patient conditions and the care delivered.
- **International alignment**: Transitioning to ICD-10 brought the U.S. closer to coding systems used globally.

## Code Structure

ICD-9-CM was composed of two main components:

1. **Diagnosis codes** (Volumes 1 and 2):  
   - Numeric and 3–5 digits in length.  
   - Example: `250.00` (Diabetes mellitus without mention of complication, type II or unspecified type, not stated as uncontrolled).

2. **Procedure codes** (Volume 3):  
   - Numeric and 2–4 digits in length.  
   - Example: `36.01` (Single vessel percutaneous transluminal coronary angioplasty [PTCA]).

### Additional Characteristics:
- Codes were hierarchical, often grouped by body system or disease category.
- Lacked alphanumeric structure and extensibility of ICD-10.
- Often supplemented with other code sets (e.g., CPT or HCPCS) for outpatient and physician billing.

> Note: Although ICD-9-CM is no longer used for new claims, legacy data coded under this system is still relevant for historical analysis, longitudinal research, and transitions of care studies.


## Maintenance Instructions

The ICD-9-CM coding system was officially retired in the United States on October 1, 2015. As such, it is no longer subject to updates or revisions. The currently available version represents the final and most recent release of this code set.
