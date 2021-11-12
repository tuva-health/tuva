{% docs __overview__ %}

# Staging
This package is where you configure your source data (claims or EHR data).  The models from this package power the rest of the Tuva Health dbt packages.  The current staging package is very lean - it only includes 4 tables and 18 total columns.

Creating the staging layer data tables requires two things:
1. Source-to-target mapping - map your raw healthcare data sources to the correct tables/columns in the staging layer
2. Normalize terminology values - map values from your raw healthcare data sources to the standard terminology (optional)

To configure each stage table, you should modify the sql file for the model so that it runs off data in your data warehouse.  The sql file currently included was built from a development environment and is for illstration purposes only (it will not run in your data warehouse).  Consult the docs or yaml files to see further details on how each stage table should be defined.

## Models
| **staging table** | **description** |
| --------------- | -------------------- |
| [patients](models/patients.sql) | One record per patient with basic demographic information. |
| [encounters](models/encounters.sql) | One record per encounter with basic administrative information and links to stg_patients. |
| [diagnoses](models/diagnoses.sql) | One record per diagnosis which links back to stg_encounters. |
| [procedures](models/procedures.sql) | One record per procedure which links back to stg_encounters. |

## Tests
This package tests your raw healthcare data several common problems including:

1. Duplicate patients and encounters
2. Referential integrity
3. Valid values for categorical data elements

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: tuva-health/staging
    version: [">=0.1.0"]
```

## Database Support
This package has been tested on Snowflake.


{% enddocs %}