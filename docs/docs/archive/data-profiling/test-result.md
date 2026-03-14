---
id: test-result
title: "Test Result"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';

The test_result table contains one record for each test that was run.  Every test being performed will have a record in this table, along with the number of failures and number of records to wich the test was applied (denominator).  It also records other metadata about theh test, such as the source table, grain of the test, category of the test, and type of claim to which the test is relevant if applicable.

<JsonDataTable jsonPath="nodes.model\.the_tuva_project\.data_profiling__test_result.columns" />