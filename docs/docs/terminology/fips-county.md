---
id: fips-county
title: "FIPS County"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

Here's a brief explanation of how FIPS works:
- Each State has a unique two digit FIPS code. For example, California is 06 and New York is 36.
- Each county has a unique five digit FIPS code. The first two digits are the state FIPS code and the last three are
the county. 
- The next level we care about is the census tract. Each tract has a unique 11 digit FIPS code. The first two digits
are the state FIPS code, the next three are the county FIPS code and the last six are the tract FIPS code.
- Finally, we have the census block group. Each block group has a unique 12 digit FIPS code. The first two digits
are the state FIPS code, the next three are the county FIPS code, the next six are the tract FIPS code and the last
digit is the block group FIPS code.

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.reference_data__fips_county.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/fips_county.csv_0_0_0.csv.gz">Download CSV</a>
