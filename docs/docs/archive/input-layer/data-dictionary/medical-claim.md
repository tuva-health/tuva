---
id: medical-claim
title: "Medical Claim"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';

The medical_claim table contains information on healthcare services and supplies provided to patients, billed by providers, and paid for by health insurers.  It includes information on the provider who rendered the service, the amount paid for the service by the health insurer, and the underlying reason for the service (i.e. diagnosis).  

The medical_claim table is at the claim-line grain i.e. it has one record per claim-line.  It combines professional claims (i.e. services billed on a CMS-1500 claim form typically by physicians) and institutional claims (i.e. services billed on a UB-04 claim form typically by hospitals or other institutions) into a single table.  

A typical medical claims dataset includes claims header information and claims line information.  Header information (e.g. DRG) only occurs once per claim whereas line information (e.g. revenue code) may occur many times per claim.  Some claims datasets have header and line information separated into distinct tables while other datasets have the information combined into a single table.  When you combine header and line information into a single table you need to repeat the values of the header data elements for every line on the claim.  

<JsonDataTable jsonPath="nodes.model\.the_tuva_project\.input_layer__medical_claim.columns" />
