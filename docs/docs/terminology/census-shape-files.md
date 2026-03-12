---
id: census-shape-files
title: "Census Shape Files"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

The U.S. Census provides geographic shape files for different grains of census areas.  We use three primary areas: County, Tract and Block Group.  You can find the original file downloads from the U.S. Census [here](https://www.census.gov/cgi-bin/geo/shapefiles/index.php).

For census tract and block groups, the U.S. Census provides shapefiles on a state by state basis.  We have preprocessed these state files to create single shape files for the entire U.S.  You can find the County, Tract and Block Group shapefiles in our S3 reference bucket here:
- [County](https://tuva-public-resources.s3.amazonaws.com/reference-data/2022+Census+Shapefiles/us-census-counties.zip)
- [Tract](https://tuva-public-resources.s3.amazonaws.com/reference-data/2022+Census+Shapefiles/us-census-tracts.zip)
- [Block Group](https://tuva-public-resources.s3.amazonaws.com/reference-data/2022+Census+Shapefiles/us-census-block-groups.zip)

The full documentation of data fields can be found [here](https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2023/TGRSHP2023_TechDoc.pdf).  Here is a brief description of the fields taken from the above full documentation: 

**Block Groups:**

| Field     | Length | Type   | Description                                                                                     |
|-----------|--------|--------|-------------------------------------------------------------------------------------------------|
| STATEFP   | 2      | String | Current state FIPS code                                                                         |
| COUNTYFP  | 3      | String | Current county FIPS code                                                                        |
| TRACTCE   | 6      | String | Current census tract code                                                                       |
| BLKGRPCE  | 1      | String | Current block group number                                                                      |
| GEOID     | 12     | String | Census block group identifier; a concatenation of the current state FIPS code, county FIPS code, census tract code, and block group number. |
| NAMELSAD  | 13     | String | Current translated legal/statistical area description and the block group number                |
| MTFCC     | 5      | String | MAF/TIGER Feature Class Code (G5030)                                                            |
| FUNCSTAT  | 1      | String | Current functional status                                                                       |
| ALAND     | 14     | Number | Current land area                                                                               |
| AWATER    | 14     | Number | Current water area                                                                              |
| INTPTLAT  | 11     | String | Current latitude of the internal point                                                          |
| INTPTLON  | 12     | String | Current longitude of the internal point                                                         |

**Census Tracts:**

| Field     | Length | Type   | Description                                                                                     |
|-----------|--------|--------|-------------------------------------------------------------------------------------------------|
| STATEFP   | 2      | String | Current state FIPS code                                                                         |
| COUNTYFP  | 3      | String | Current county FIPS code                                                                        |
| TRACTCE   | 6      | String | Current census tract code                                                                       |
| GEOID     | 12     | String | Census block group identifier; a concatenation of the current state FIPS code, county FIPS code, census tract code, and block group number. |
| NAMELSAD  | 13     | String | Current translated legal/statistical area description and the block group number                |
| MTFCC     | 5      | String | MAF/TIGER Feature Class Code (G5030)                                                            |
| FUNCSTAT  | 1      | String | Current functional status                                                                       |
| ALAND     | 14     | Number | Current land area                                                                               |
| AWATER    | 14     | Number | Current water area                                                                              |
| INTPTLAT  | 11     | String | Current latitude of the internal point                                                          |
| INTPTLON  | 12     | String | Current longitude of the internal point                                                         |

