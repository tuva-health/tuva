---
id: ndc
title: "NDC"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

NDC to RxNorm description (with RXCUI) and FDA description.

NOTE: If RxNorm and RxNorm Historical have same NDCs, we prefer RxNorm. If any of the 3 FDA sources have same NDCs, we prefer FDA NDC, FDA Excluded, FDA Unfinished in that order.

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__ndc.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ndc.csv_0_0_0.csv.gz">Download CSV</a>

## Maintenance Instruction

1. We are sourcing data from [CodeRx](https://coderx.io/). Please check [all_ndc_descriptions](https://coderxio.github.io/sagerx/#!/model/model.sagerx.all_ndc_descriptions) dbt docs for detailed processes.
2. Unload the table to S3 as a `.csv` file (requires credentials with write permissions to the bucket):
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/ndc.csv
from [table_from_step_2]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
overwrite = true;
```
3. Create a branch in [docs](https://github.com/tuva-health/docs). Update the `last_updated` column in the table above with the current date
4. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents of the terminology file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Copy and paste the updated header into the [ndc file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__ndc.csv)
3. Submit a pull request
