---
id: header-line
title: "Headers and Lines"
description: This section discusses elements commonly found on the Claim Form Header vs. those contained on individual Claim Form Lines, and how to interpret and analyze them. 
---
## Claim Headers

The claim header is the top-level portion of a claim. It contains information that applies to the entire claim regardless of the number of services (claim lines) included. It lets you track who, when, and at what overall cost the services occurred.

Typical data elements:

- Patient-level info: Patient ID, DOB, gender.
- Subscriber-level info: Member/Subscriber ID, relationship to subscriber.
- Provider info: Billing provider NPI, taxonomy, billing address.
- Claim-level info: Claim number, payer claim control number, claim received date, claim type (professional vs institutional), total billed amount, total allowed amount, paid amount.
- Insurance info: Plan ID, payer ID, coverage type.
- Admission/discharge info (for UB-04/institutional claims).

## Claim Lines

A claim line is a single itemized service or charge that falls under the claim header. A claim usually contains multiple lines, and the lines tell you what specifically was done/provided during the encounter and the payment detail at the service level.

Typical data elements:

- Service-level info: CPT/HCPCS code (procedure), modifiers, revenue codes (for UB-04), line billed amount, line allowed/paid amounts.
- Diagnosis pointers: Links to the ICD-10 codes on the header to show why the service was performed.
- Service dates: From/to dates (can differ line by line).
- Units of service: E.g., 5 units of physical therapy or 30 days of medication.
- Rendering provider info (if different from billing provider).

## Professional Claims (CMS-1500)

Used by: Individual providers and clinics (physicians, therapists, durable medical equipment suppliers).

Claim Header:

- Patient/subscriber info: Name, DOB, subscriber ID, relationship.
- Billing provider info: NPI, address, taxonomy.
- Payer info: Insurance plan, payer ID.
- Claim control number: For tracking.
- Total charge amount: Sum of all lines.

Claim Lines:

- CPT/HCPCS procedure codes (with modifiers if needed).
- Diagnosis pointers (e.g., Line 1 points to ICD-10-CM codes A + B).
- Units (time-based services, number of items dispensed).
- Line billed, allowed, paid amounts.
- Rendering provider NPI (if different from billing provider).

Key Feature: The focus is on procedure codes (CPT/HCPCS) because the claim is about what the provider did.

## Institutional Claims (UB-04)

Used by: Hospitals, skilled nursing facilities, home health agencies, outpatient facilities.

Claim Header:

- Patient info: Same as professional.
- Billing provider info: Facility NPI, type of bill.
- Admission/discharge dates, patient status (e.g., discharged home, expired).
- Claim totals: Total charges, covered charges, non-covered charges.
- Occurrence, condition, and value codes (special billing info like accident dates, patient liability).

Claim Lines:

- Revenue codes: Identify the type of department/service (e.g., 0450 = Emergency Room, 0300 = Laboratory).
- HCPCS/CPT codes (optional, used especially for outpatient).
- Units (e.g., 3 days of room & board, 2 hours of observation).
- Line-level charges.
- Service dates (per line).

Key Feature: The focus is on facility services and resources used (via revenue codes), with CPT/HCPCS used for outpatient detail.

## How They Work Together

The header is the “envelope” for the claim; the lines are the “contents.” When analyzing data, use header-level fields for identifying the claim and overall cost; use line-level fields for utilization (types of services, coding detail, units).

- Professional (CMS-1500) = itemized physician/provider services (what a doctor did).
- Institutional (UB-04) = itemized facility services (where the patient was and what resources were used).

Example:

- Claim Header: Patient X, Dr. Smith, 01/15/2025, billed $1,200, total allowed $850.
- Claim Lines:
  - Line 1: CPT 99213 (office visit), billed $150, allowed $90.
  - Line 2: CPT 80053 (comprehensive metabolic panel), billed $200, allowed $150.
  - Line 3: CPT 93000 (ECG), billed $100, allowed $80.

Together, those three lines add up to the amounts summarized at the header.

Example scenario: Patient goes to ER and receives a physician exam and a CT scan. The payer receives two separate claims (professional + institutional) for the same encounter.

- Institutional claim (UB-04): ER facility fee (rev code 0450), CT scan facility charge (rev code 0350).
- Professional claim (CMS-1500): Physician exam CPT 99285, radiologist read CPT 70450.
