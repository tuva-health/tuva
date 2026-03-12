---
id: terminology
title: "6. Terminology"
hide_title: true
description: The Tuva Project makes it easy to load useful terminology sets like ICD-10 codes directly into your data warehouse where you need them for analytics.
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

# 6. Terminology 

Terminology includes code sets (e.g. ICD-10-CM, HCPCS), value sets (e.g. the ICD-10-CM codes that define Type 2 Diabetes), and reference datasets (e.g. provider NPI lookups).

These datasets are scattered all over the internet, maintained in different data formats, and updated on different frequencies.  The Tuva Community maintains these datasets and keeps them up to date.  

When you run Tuva the Terminology datasets are automatically loaded into your cloud data warehouse.

The Tuva Project leverages Terminology in data quality tests (e.g. are these ICD-10-CM codes valid?) and in data marts (e.g. definintions for chronic conditions).

Use the [Terminology Viewer](https://terminology.thetuvaproject.com) to view and search through all the datasets included in Terminology.


<!-- The **Last Updated** date in the table below is the date the codeset was released by maintainer or if not available, the date we loaded it to Tuva.

<table>
  <thead>
    <tr>
      <th>Terminology Set</th>
      <th>Maintainer</th>
      <th>Update Frequency</th>
      <th>Last Updated</th>
      <th>Download CSV</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="../terminology/act-site">Act Site</a></td>
      <td>HL7 International Vocabulary/Terminology Work Group</td>
      <td></td>
      <td>8/20/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/act_site.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/admit-source">Admit Source</a></td>
      <td>National Uniform Billing Committee</td>
      <td></td>
      <td>4/22/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/admit_source.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/admit-type">Admit Type</a></td>
      <td>National Uniform Billing Committee</td>
      <td></td>
      <td>4/22/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/admit_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/ansi-fips-state">ANSI FIPS State</a></td>
      <td>ANSI</td>
      <td></td>
      <td></td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ansi_fips_state.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/appointment-cancellation-reason">Appointment Cancellation Reason</a></td>
      <td>HL7</td>
      <td></td>
      <td>8/25/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/appointment_cancellation_reason.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/appointment-status">Appointment Status</a></td>
      <td>HL7</td>
      <td></td>
      <td>12/28/2020</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/appointment_status.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/appointment-type">Appointment Type</a></td>
      <td>HL7</td>
      <td></td>
      <td>12/01/2019</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/appointment_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/apr-drg">APR-DRG</a></td>
      <td>Solventum</td>
      <td></td>
      <td>4/23/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/apr_drg.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/bill-type">Bill Type</a></td>
      <td>National Uniform Billing Committee</td>
      <td></td>
      <td>11/3/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/bill_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/calendar">Calendar</a></td>
      <td>Tuva</td>
      <td></td>
      <td></td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/calendar.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/census-shape-files">Census Shape Files</a></td>
      <td>U.S. Census</td>
      <td>Update Annually</td>
      <td></td>
      <td><a></a></td>
    </tr>
    <tr>
      <td><a href="../terminology/claim-type">Claim Type</a></td>
      <td>Tuva</td>
      <td></td>
      <td>11/4/2023</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/claim_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/code-type">Code Type</a></td>
      <td>Tuva</td>
      <td></td>
      <td>4/19/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/code_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/cvx">CVX</a></td>
      <td>U.S. Centers for Disease Control and Prevention(CDC)</td>
      <td>Regularly</td>
      <td>08/20/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/cvx.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/discharge-disposition">Discharge Disposition</a></td>
      <td>National Uniform Billing Committee</td>
      <td></td>
      <td>4/22/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/discharge_disposition.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/encounter-type">Encounter Type</a></td>
      <td>Tuva</td>
      <td></td>
      <td>6/17/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/encounter_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/ethnicity">Ethnicity</a></td>
      <td>Tuva</td>
      <td></td>
      <td>11/3/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ethnicity.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/fips-county">FIPS County</a></td>
      <td>Tuva</td>
      <td></td>
      <td>4/19/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/fips_county.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/gender">Gender</a></td>
      <td>Tuva</td>
      <td></td>
      <td>4/19/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/gender.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/hcpcs-level-2">HCPCS Level II</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Updated quarterly: January, April, July, and October</td>
      <td>9/24/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/hcpcs_level_2.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/hcpcs-to-rbcs">HCPCS to RBCS</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Annually</td>
      <td>8/26/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/hcpcs_to_rbcs.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/icd-9-cm">ICD-9-CM</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>No New Updates</td>
      <td>04/22/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_9_cm.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/icd-9-pcs">ICD-9-PCS</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>No New Updates</td>
      <td>04/22/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_9_pcs.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/icd-10-cm">ICD-10-CM</a></td>
      <td>Centers for Disease Control and Prevention (CDC)</td>
      <td>Annually occuring from Oct 1</td>
      <td>10/07/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_10_cm.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/icd-10-pcs">ICD-10-PCS</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Semi-annually occuring from (Oct 1 to Mar 31) and (Apr 1 to Sept 31)</td>
      <td>10/07/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_10_pcs.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/immunization-route-code">Immunization Route Code</a></td>
      <td>HL7 International Vocabulary/Terminology Work Group</td>
      <td></td>
      <td>8/20/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/immunization_route_code.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/immunization-status">Immunization Status</a></td>
      <td>HL7 International Vocabulary/Terminology Work Group</td>
      <td></td>
      <td>8/20/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/immunization_status.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/immunization-status-reason">Immunization Status Reason</a></td>
      <td>HL7 International Vocabulary/Terminology Work Group</td>
      <td></td>
      <td>8/20/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/immunization_status_reason.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/loinc">LOINC</a></td>
      <td>Regenstrief Institute</td>
      <td>Released twice a year, in February and August</td>
      <td>2/26/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/loinc.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/loinc-deprecated-mapping">LOINC Deprecated Mapping</a></td>
      <td>Regenstrief Institute</td>
      <td>Released twice a year, in February and August</td>
      <td>2/26/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/loinc_deprecated_mapping.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/mdc">MDC</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Annually occuring from April 1</td>
      <td>10/09/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/mdc.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/medicare-dual-eligibility">Medicare Dual Eligibility</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td></td>
      <td>3/7/2023</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/medicare_dual_eligibility.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/medicare-orec">Medicare OREC</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td></td>
      <td>9/25/23</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/medicare_orec.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/medicare-status">Medicare Status</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td></td>
      <td>11/3/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/medicare_status.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/ms-drg-weights-los">MS DRG Weights and LOS</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td></td>
      <td>8/26/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ms_drg_weights_los.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/ms-drg">MS-DRG</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Semi-annually, one effective for Oct 1 to Mar 31 and another for Apr 1 to Sept 31</td>
      <td>10/09/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ms_drg.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/ndc">NDC</a></td>
      <td><a href="https://coderx.io/">CodeRx</a></td>
      <td>Weekly Update</td>
      <td>8/25/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ndc.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/other-provider-taxonomy">Other Provider Taxonomy</a></td>
      <td>NUCC / CMS</td>
      <td>Monthly</td>
      <td>09/08/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_provider_data/latest/other_provider_taxonomy_compressed.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/payer-type">Payer Type</a></td>
      <td>Tuva</td>
      <td></td>
      <td>4/19/2022</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/payer_type.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/place-of-service">Place of Service</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Updated periodically; no fixed schedule provided</td>
      <td>2/5/2024</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/place_of_service.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/present-on-admission">Present on Admission</a></td>
      <td>Centers for Medicare & Medicaid Services (CMS)</td>
      <td>Updated periodically; no fixed schedule provided.</td>
      <td>04/15/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/present_on_admission.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/provider">Provider</a></td>
      <td>NPPES</td>
      <td>Monthly</td>
      <td>09/08/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_provider_data/latest/provider_compressed.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/race">Race</a></td>
      <td>Tuva</td>
      <td></td>
      <td>2/3/2023</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/race.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/revenue-center">Revenue Center</a></td>
      <td>National Uniform Billing Committee</td>
      <td>Updated periodically; no fixed schedule provided.</td>
      <td>15/4/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/revenue_center.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/rxnorm-to-atc">RxNorm to ATC</a></td>
      <td><a href="https://coderx.io/">CodeRx</a></td>
      <td>Weekly Update</td>
      <td>8/25/2025</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/rxnorm_to_atc.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/snomed-ct">Snomed-CT</a></td>
      <td>US National Library of Medicine</td>
      <td></td>
      <td>3/1/2024</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/snomed_ct_compressed.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/snomed-ct-transitive-closures">Snomed-CT transitive closures</a></td>
      <td>US National Library of Medicine</td>
      <td></td>
      <td>3/1/2024</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/snomed_ct_transitive_closures_compressed.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/snomed-ct-to-icd-10-cm-map">Snomed-CT to ICD-10-CM Map</a></td>
      <td>US National Library of Medicine</td>
      <td></td>
      <td>9/1/2023</td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/snomed_icd_10_map.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/social-vulnerability-index">Social Vulnerability Index</a></td>
      <td>Centers for Disease Control and Prevention</td>
      <td></td>
      <td>06/20/2025</td>
      <td><a></a></td>
    </tr>
    <tr>
      <td><a href="../terminology/ssa-state-fips">SSA State FIPS</a></td>
      <td>Social Security Administration</td>
      <td></td>
      <td></td>
      <td><a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ssa_fips_state.csv_0_0_0.csv.gz">Link</a></td>
    </tr>
    <tr>
      <td><a href="../terminology/zip-code">Zip Code</a></td>
      <td>U.S. Census / HUD</td>
      <td>Updated annually, typically in September alongside TIGER/Line Shapefiles</td>
      <td></td>
      <td><a></a></td>
    </tr>
  </tbody>
</table>

# File Download/Extraction Help
Built-in file managers (e.g. Archive Utility on MacOS) may have some trouble decompressing files downloaded directly from this terminology page. You may encounter an error complaining that the files are in an unsupported file format.

One way to get around these errors is by using [`gzip`](https://www.gzip.org/), which will help you decompress these files without encountering the same unsupported file format errors you were seeing before.

```console
# would extract terminology_file.csv to the same directory
gzip -d path/to/your/file/terminology_file.csv.gz 
``` -->
