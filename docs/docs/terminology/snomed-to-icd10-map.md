---
id: snomed-to-icd10-map
title: "SNOMED-CT to ICD-10 Map"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__snomed_icd_10_map.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/snomed_icd_10_map.csv_0_0_0.csv.gz">Download CSV</a>

## Maintenance Instructions

This mapping is updated with each new relase of SNOMED CT US Edition which 
happens in March and September, and includes the annual ICD-10-CM update.

The mapping file can be found on the [SNOMED CT United States Edition](https://www.nlm.nih.gov/healthit/snomedct/us_edition.html)
page. Click on the link to download the SNOMED CT to ICD-10-CM Mapping Resources
which includes the human-readable version that contains all required data 
elements in a single TSV file.

The only clean-up required for the Tuva project is to remove the formatting
from the maptarget (ICD-10-CM code) field (e.g. `replace(maptarget,'.','')`).

The HCC Suspecting data mart utilizes the default mapping guidance from NLM which
specifies that the map priority rule of “TRUE” or “OTHERWISE TRUE” should be 
applied if nothing further is known about the patient’s condition. Other 
use-cases may need to further evaluate the map rules that consider a patient's
age, gender, and comorbidities.
