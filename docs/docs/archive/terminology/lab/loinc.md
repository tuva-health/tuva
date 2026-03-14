---
id: loinc
title: "LOINC"
---


[//]: # (import { CSVDataTable } from '@site/src/components/CSVDataTable';)
[//]: # (import { JsonDataTable } from '@site/src/components/JsonDataTable';)


LOINC (Logical Observation Identifiers Names and Codes) is a code set for classifying measurements, observations, and documents. The codes attempt to cover anything that can be tested, measured, or observed about a patient.   It is maintained by the Regenstrief Institute.
LOINC codes represent the "question" for a test or measurement.
The codes are of the form XXXXX-Y, with any number of digits before the hyphen.  The digit after the hyphen is a [check digit](https://loinc.org/kb/users-guide/calculating-mod-10-check-digits/) that can be calculated from the other digits, and is used to validate that the code was entered correctly.

There are 6 different dimensions that make up the LOINC test, which LOINC calls "parts".  If any part is different between two tests, they will have different LOINC codes.

The following is a breakdown of the different LOINC parts, with examples for **`806-0: manual count of white blood cells in cerebral spinal fluid specimen`**
- **Component**: Leukocytes (white blood cells)
- **Property**: NCnc (Number concentration)
- **Time**: PT (Point in time)
- **Specimen/System**: CSF (Cerebral spinal fluid)
- **Scale**: Qn (Quantitative)
- **Method** (optional): Manual Count 

In addition to the code set, LOINC produces a map of deprecated or discouraged LOINC codes to their active counterparts.


[//]: # (the structure in manifest.json for the column descriptions is different for seeds than models. )

[//]: # (we either need a new function or some conditional logic to pull in the descriptions and data types)

[//]: # (<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__loinc.columns" />)






[//]: # (<CSVDataTable csvUrl="https://github.com/tuva-health/the_tuva_project/blob/main/seeds/terminology/terminology__loinc.csv" />)