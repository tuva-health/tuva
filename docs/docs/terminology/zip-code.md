---
id: zip-code
title: "Zip Code"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

In some cases you might want to use zipcode as a grain. There is not a one-to-one relationship between zipcodes
and tracts but you can use the 
[Census Zip Code Tabulation Areas (ZCTA)](https://www.census.gov/geographies/reference-maps/2010/geo/2010-zcta-rel.html)
or the HUD [ZIP Code Crosswalk Files](https://www.huduser.gov/portal/datasets/usps_crosswalk.html). For convenience,
we also host the crosswalks in our reference bucket: 
- [Zip to Tract](https://tuva-public-resources.s3.amazonaws.com/reference-data/Crosswalks/zip_to_tract.csv_0_0_0.csv)
- [Tract to Zip](https://tuva-public-resources.s3.amazonaws.com/reference-data/Crosswalks/tract_to_zip.csv_0_0_0.csv)
