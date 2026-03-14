---
id: other-provider-taxonomy
title: "Other Provider Taxonomy"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__other_provider_taxonomy.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_provider_data/latest/other_provider_taxonomy_compressed.csv.gz">Download CSV</a>

## Maintenance Instructions

FYI: Terminologies `Provider` and `Other Provider Taxonomy` both will be updated monthly simultaneously from same source data

1. Download the latest source files from the following locations:

   - **NPPES Data Dissemination** (monthly):  
     [https://download.cms.gov/nppes/NPI_Files.html](https://download.cms.gov/nppes/NPI_Files.html)

   - **NUCC Health Care Provider Taxonomy** (semi-annually in January and July):  
     [https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57](https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57)

   - **CMS Medicare Provider and Supplier Taxonomy Crosswalk** (annually):  
     [https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-provider-and-supplier-taxonomy-crosswalk](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-provider-and-supplier-taxonomy-crosswalk)

2. Load the downloaded data into your data warehouse. These three files should be in three tables in a schema of your database.
    - NPPES NPI Data (Note: source data comes zipped with many files, only the "npidata_pfile....csv" and "othername_pfile....csv" are required.)
        - [Snowsql](https://docs.snowflake.com/en/user-guide/snowsql) may be used to load large file from local machine to snowflake.
    - NUCC Health Care Provider Taxonomy
    - CMS Medicare Provider and Supplier Taxonomy Crosswalk

3. Follow the steps outlined in the README here:  
   [https://github.com/tuva-health/provider](https://github.com/tuva-health/provider)

You should have exported the seed to an S3 bucket by now

4. Create a branch in [docs](https://github.com/tuva-health/docs). Update the `last_updated` column in the table above with the current date
5. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents of the terminology file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Copy and paste the updated header into the [Other Provider Taxonomy file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__other_provider_taxonomy.csv)
3. Submit a pull request
