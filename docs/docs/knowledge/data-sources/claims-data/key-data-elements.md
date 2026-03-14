---
id: key-data-elements
title: "Key Data Elements"
description: This section describes the fundamentals of many atomic-level claims data elements.
---

Claims data is made up of several data elements that are important to analytics.  Here we describe the key data elements in some detail.

## Administrative Fields

One of the great things about claims data (compared to clinical data) is that they contain a number of fields with standard terminology that are (usually) well-populated.  We refer to these fields as administrative or billing codes because they are used for billing (i.e. administrative) purposes as opposed for clinical purposes.  These fields contain valuable information about the services performed, where they were performed, and where the patient came from and went to before/after the service.  

In this section we review these codes, their analytic use cases, and how you can identify common data quality problems in them.

### Admit Source
Admit source code is used in institutional claims to indicate where the patient was located prior to admission.  The field does not exist in professional claims.  The field exists at the header-level, meaning there should be only 1 distinct value for this field per claim.

Admit source, along with admit type, is the least reliable among the administrative codes because the accuracy of the code is not verified during the claims adjudication process (other than verifying that the code is in fact a valid code).

Despite this, it's possible to use admit source to help identify things like:
- transfers from another hospital
- inpatient stays that came through the emergency department

Admit source codes are maintained by the National Uniform Billing Committee (NUBC).

You can find a complete listing of admit source codes and their descriptions [here](/terminology/admit-source).

### Admit Type
Admit type code is used in institutional claims to indicate the priority of admission, e.g., urgent, emergent, elective, etc.  The field does not exist in professional claims.  The field exists at the header-level, meaning there should be only 1 distinct value for this field per claim.

Admit type along with admit source, is the least reliable among the administrative codes because the accuracy of the code is not verified during the claims adjudication process (other than verifying that the code is in fact a valid code).

Despite this, admit type is commonly used to identify things like elective procedures.

Admit type codes are maintained by the National Uniform Billing Committee (NUBC).

You can find a complete listing of admit type codes and their descriptions [here](/terminology/admit-type).

### Bill Type
Bill type code is by far the most complex of the administrative codes.  Each digit in the bill type code has a distinct purpose and meaning:

- 1st digit: This is always "0" and often omitted.
- 2nd digit: Indicates the type of facility, e.g., skilled nursing facility 
- 3rd digit: Indicates the type of care, e.g., inpatient part A
- 4th digit: Indicates the sequence of the bill (also referred to as the frequency code)

The thing that makes this code complex is that the possible values of the 3rd and 4th digits depend on the value of the 2nd digit.  As a result, some claims datasets will separate out the digits of bill type code into distinct fields.  However, we find it preferable to work with bill type code as a single field and the dictionary below lists all bill type codes this way.

Despite the complexity of this field, it's extremely useful.  Bill type code is used extensively in the creation of service categories, including the identification of acute inpatient, outpatient, skilled nursing, and emergency department services, among many others.  The field is generally considered reliable because the accuracy and suitability of the code is verified during the claims adjudication process, i.e., a claim may be denied if the code doesn't make sense.

Bill type codes are maintained by the National Uniform Billing Committee (NUBC).

You can find a complete listing of bill type codes and their descriptions [here](/terminology/bill-type).

### Discharge Disposition
Discharge disposition code indicates where the patient was discharged following a stay at a facility.  The field only exists on institutional claims.  The field is sometimes called discharge status or patient status.  The field exists at the header-level, meaning there should be only 1 distinct value for this field per claim.

The code is commonly used to identify things like:
- Patients that died during an institutional stay
- Patients who were transferred
- Patients who were discharged to home or home w/ home health services
- Patients who left against medical advice (LAMA)

Discharge disposition codes are maintained by the National Uniform Billing Committee (NUBC).

You can find a complete listing of discharge disposition codes and their descriptions [here](/terminology/discharge-disposition).

### HCPCS
HCPCS codes indicate the services and supplies rendered by providers to patients.  These codes are used in both institutional and professional claims forms.  These codes exist at the line-level, meaning there can be many HCPCS codes on a single claim.  There are codes for many different types of supplies and services including:
- physician visits
- lab tests
- imaging reads
- durable medical equipment
- remote patient monitoring devices

And many many other types of things.  There are thousands of HCPCS codes spread across two levels.  Level 1 codes, also called CPT codes, are maintained by the American Medical Association (AMA).  Level 2 codes are maintained by CMS.

Professional contracted rates between payers and providers are established using HCPCS codes.  These rates are referred to as a fee schedule.  Conversely, institutional rates are often paid on a per encounter (e.g. DRG) or per diem basis.

You can find a complete listing of all level 2 HCPCS codes and their descriptions [here](/terminology/hcpcs-level-2).

### Place of Service
Place of service codes indicate the type of care setting professional claim services were delivered in.  This field only exists on professional claims.  Place of service is coded at the line-level to reflect the fact that services during a particular encounter can occur in different locations.  Because of this, a single professional claim can have multiple place of service codes.

Place of service codes are used to assign claims to services categories.  For example, place of service code 11 indicates an office visit.

CMS maintains place of service codes.

You can find a complete listing of all place of service codes and their descriptions [here](/terminology/place-of-service).

### Revenue Center Codes
Revenue center codes are used to account for the services and supplies rendered to patients in institutional care settings.  These codes are only used in institutional claims.  Typically these codes will correspond to a facility's chargemaster, which is a listing of all charges used by the institution in billing.  Although a hospital will use these codes to "charge" the health insurer, they have no bearing on the contracted payment amount, i.e., the amount paid to the provider by the payer.  The payment amount is entirely determined by MS-DRG for inpatient claims and often a per diem rate for skilled nursing.

Many different categories of revenue center codes exist including for example:
- Room and Board
- Emergency
- IV Therapy

For a given institutional claim there may be dozens of revenue center codes used.  These codes are submitted at the line-level of the claim, so there is no limit to the number of revenue center codes that may be used on a given claim.

Revenue center codes play an important role in identifying different types of insitutional claims, including acute inpatient, emergency department, and others.

Revenue center codes are maintained by the National Uniform Billing Committee (NUBC).

You can find a complete listing of revenue center codes and their descriptions [here](/terminology/revenue-center).

## Date Fields

Claims data is longitudinal in nature i.e. it captures conditions, services and other healthcare events that occur over time to patients.  This makes claims data extremely useful for analyzing sequences of events e.g. did patients who received drug X have better or worse outcomes?  However the ability to reliably use claims data in this matter is predicated by the completeness and accuracy of a variety of key date fields found in claims data.

The date fields listed below are the names we give to these fields in the Tuva Project, but there can be all sorts of different names for these fields in different claims datasets.  For example, in Medicare LDS the claim_end_date field is called clm_thru_dt.

### Medical Claims

To understand the key date fields in medical claims, it's useful to consider an example of a patient who's been receiving care in a long-term care (i.e. skilled nursing) facility for 1 year, from January 1st to December 31st, and suppose the facility bills the insurer every month on the beginning of the month.

- **claim_start_date:** The start date of the billable period for the claim.  In the example above this date would always be the first date of the month.
- **claim_end_date:** The end date of the billable period for the claim.  In the example above this date would always be the last date of the month.
- **admission_date:** The date the patient was first admitted to the facility.  In the example above this date would be January 1st.  This field only exists on institutional claims, not professional.
- **discharge_date:**  The date the patient was discharged from the facility.  In the example above this date would be December 31st.  This field only exists on institutional claims, not professional.
- **paid_date:**  The date the claim was paid by the insurance company.  This date could be any date after the claim_end_date.  Often this date is within a couple weeks of claim_end_date.

There are 2 other date fields in medical claims.  They are claim_line_start_date and claim_line_end_date.  These date fields are less important - in fact we don't currently use them in any analytics in the Tuva Project.

### Pharmacy Claims

- **dispensing_date:** The date when the prescription was filled by the pharmacy and given to the patient.
- **paid_date:**  The date the claim was paid.  Often this date lags the dispensing_date by days or weeks.

### Eligibility

- **enrollment_start_date:** The date when a patient became enrolled in a health plan (i.e. insurance).  Patients can gain and lose enrollment over time, so a given patient may have more than one enrollment_start_date.
- **enrollment_end_date:** The date when a patient loses enrollment in a health plan (i.e. insurance).  Patients can gain and lose enrollment over time, so a given patient may have more than one enrollment_end_date.  Patients who are currently enrolled will not have an enrollment_end_date, or they may have a long-dated enrollment_end_date e.g. 12/31/9999, which is meant to indicate they are still enrolled.
- **birth_date:**  The date the patient was born.  
- **death_date:**  The date the patient died (if applicable).  Just because a patient does not have a death date does not mean they aren't deceased!  Many deaths do not occur in a healthcare facility and therefore are not captured in claims.  Sometimes the death date is captured in eligibility data, but often it is inferred by discharge_disposition_code = 20 (this field is found in institutional claims). 

### Data Quality Issues with Claims Date Fields

As you might expect, the date fields in claims often suffer from data quality issues.  For example, date fields can be missing, or the dates can exist unnaturally far into the past or into the future.

Identifying these sorts of problems across all the key date fields can be challenging and require a lot of ad hoc querying.  We've figured out a good way to look for these sorts of data quality problems and built it into the Tuva Project.  Check out the code and video below for more info.

```sql
select *
from insights.count_claim_by_date_column
order by 1
```

<iframe width="640" height="400" src="https://www.youtube.com/embed/QE9N5FqeNd4?si=iRPvidLj43JwY7ag" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

## Financial Fields

## Provider Fields

Medical claims includes several fields containing information on providers. The fields vary based on the type of claim.

**Institutional Claims [CMS-1450 or UB-04](https://www.cdc.gov/wtc/pdfs/policies/ub-40-P.pdf):**
Provider information in the header of facility claims. In addition to the facility billing the service, these claims contain several fields for NPIs from up to four individual providers involved in the care (e.g., Attending Physician).
- Box 1 Billing Provider Name and Address
- 2 Pay-to Proivder Name and Address
- 5  Federal Tax ID
- 76 Attending Physician
- 56 Billing Provider NPI
- 57 Other Provider ID
- 77 Operating Physician
- 78 Other Physician
- 79 Other Physician

**Professional Claims [CMS-1500](https://www.cms.gov/medicare/cms-forms/cms-forms/downloads/cms1500.pdf):** 
Professional claims track the NPI of the provider who rendered each individual line item (i.e., CPT/HCPSCS code) in the claim. In addition, the claim header contains information on the organization submitting the claim. 
- Box 17  Referring Provider
- 24J Rendering Provider
- 25 Federal Tax ID
- 32 Service Facility Location Information
- 33 Billing Provider