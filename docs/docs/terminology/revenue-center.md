---
id: revenue-center
title: "Revenue Center"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__revenue_center.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/revenue_center.csv_0_0_0.csv.gz">Download CSV</a>

### What are Revenue Center Codes?

Revenue Center Codes (also called Revenue Codes) are **4-digit codes used on institutional (facility) claims** to identify specific accommodations, ancillary services, or billing centers. They help categorize charges and associate services with the appropriate cost center within a hospital or other healthcare facility.

Each revenue code reflects **where** or **how** a service was provided, but not the actual service itself—that’s the role of HCPCS or CPT codes that may be paired with it.

---

### On what kind of claims are Revenue Center Codes found?

Revenue Center Codes appear on:

- **Institutional claims** only  
  - **UB-04 (837I)** claim forms from:
    - Inpatient hospitals
    - Outpatient hospitals
    - Skilled nursing facilities
    - Home health agencies
    - Hospice agencies

They are **not present** on professional claims (837P).

---

### How often are Revenue Center Codes updated?

Revenue codes are maintained by the **National Uniform Billing Committee (NUBC)**. Updates are published as part of the UB-04 Data Specifications Manual and are typically released **annually**, although additions and revisions can occur more frequently.

---

### Code Structure

- **4-digit numeric code**: The first digit is often zero-padded but can also have meaning in some contexts (e.g., 0xxx vs. 1xxx).  
  - Example: `0450` = Emergency Room, General Classification  
  - Example: `0278` = Medical/Surgical Supplies – Other Implants

Each code may be associated with:
- A **description**
- An **associated HCPCS/CPT code** (optional but often required)
- A **unit count**
- A **charge amount**

---

### Notes for Analysts

- **Many lines per claim**: Revenue center codes are reported at the **line level**, meaning a single institutional claim may contain multiple revenue codes for different services.
- **Tied to billing/cost centers**: While not clinical in nature, revenue codes can indicate types of services or departments that are useful in cost and utilization analyses.
- **Paired with HCPCS/CPT**: Often used in conjunction with a HCPCS/CPT code to describe what was done and where.
- **Some codes imply a service**: Certain revenue codes (e.g., 0360 for Operating Room) may imply a service type even without an accompanying HCPCS code.
- **Required for MS-DRG/APR-DRG assignment**: Revenue codes, especially room and board charges, often help define the claim setting, which is important for grouper logic.

---

### Key Use Cases

- **Identifying types of services provided** (e.g., dialysis vs. ICU stay vs. radiology)
- **Segregating inpatient vs. outpatient components of care**
- **Cost and utilization analysis by department or service category**
- **Flagging high-cost devices or implants** (e.g., 0278, 0624)
- **Identifying line-level services in episodes of care or bundled payment models**
- **Determining outpatient visit type or emergency room usage**

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

**The below description outlines the update process as it existed prior to changes in the ResDac site no longer publishing updates to this code set. Updates are currently on hold until a new source can be identified**