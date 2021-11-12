{% docs __overview__ %}

# Chronic Conditions

This package creates patient-level chronic condition flags based on the definitions from the CMS Chronic Conditions Warehouse (CCW).  The package identifies and flags 69 different chronic conditions grouped into 9 clinical areas.

There are two main output tables from this package: 1) A 'long' table with one record per patient-condition and 2) A 'wide' table with one record for patient and each condition as a separate column.

## Models 
This package contains transformation models, designed to work against four input tables: patients, encounters, diagnoses, and procedures.  These input tables are described further in the configuration section below. 

| **model** | **description** |
| --------------- | -------------------- |
| [condition_logic_simple](models/chronic_conditions/condition_logic_simple.sql) | Joins diagnosis and procedure codes from stg_diagnoses and stg_procedures to the proper codes in [chronic_conditions](data/chronic_conditions.csv). |
| [condition_logic](models/chronic_conditions/condition_logic.sql) | Joins diagnosis and procedure codes from stg_diagnoses and stg_procedures to the proper codes in [chronic_conditions](data/chronic_conditions.csv).  Conditions identified using this logic require additional criteria (e.g. only consider primary diagnosis). |
| [stroke_transient_ischemic_attack](models/chronic_conditions/stroke_transient_ischemic_attack.sql) | This logic specifically identifies patients who have experienced a stroke or TIA (mini-stroke) by joining diagnosis codes to [chronic_conditions](data/chronic_conditions.csv). |
| [benign_prostatic_hyperplasia](models/chronic_conditions/benign_prostatic_hyperplasia.sql) | This logic specifically identifies patients who have experience benign prostatic hyperplasia (also known as prostate gland enlargement) by joining to diagnosis codes in [chronic_conditions](data/chronic_conditions.csv). |
| [union_calculations](models/chronic_conditions/union_calculations.sql) | Unions the four condition logic models together and calculates measures (i.e. date of onset, most recent diagnosis date, and number of distinct encounters with the diagnosis). |
| [condition_pivot](models/chronic_conditions/condition_pivot.sql) | Pivots union_calculations to create a 'wide' table, i.e. one record per patient with 69 columns, one for each chronic condition (the values of these columns are either 1 if the patient has the condition or 0 otherwise). |

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: tuvahealth/chronic_conditions
    version: [">=0.1.0"]
```

## Configuration
This package requires you to configure 4 stage tables.  These 4 stage tables are designed to run all Tuva Health dbt packages, so you only need to configure them once to run all packages.

To configure each stage table, you should modify the sql file for the model so that it runs off data in your data warehouse.  The sql file currently included was built from a development environment and is for illstration purposes only (it will not run in your data warehouse).  Consult the docs or yaml files to see further details on how each stage table should be defined.

| **stage table** | **description** |
| --------------- | -------------------- |
| [stg_patients](models/stage/stg_patients.sql) | One record per patient with basic demographic information. |
| [stg_encounters](models/stage/stg_encounters.sql) | One record per encounter with basic administrative information and links to stg_patients. |
| [stg_diagnoses](models/stage/stg_diagnoses.sql) | One record per diagnosis which links back to stg_encounters. |
| [stg_procedures](models/stage/stg_procedures.sql) | One record per procedure which links back to stg_encounters. |

## Contributions
Don't see a model or specific metric you would have liked to be included? Notice any bugs when installing 
and running the package? If so, we highly encourage and welcome contributions to this package! 
Please create issues or open PRs against `master`. See [the Discourse post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) for information on how to contribute to a package.

## Database Support
This package has been tested on Snowflake.


{% enddocs %}