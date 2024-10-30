## Helper Scripts

This folder contains scripts that help with various functions related to The Tuva Project

### schema_extract.py

This script retrieves a list of schemas, tables, and columns from the tuva project on github.  Run with no parameters to fetch the latest release, or pass the tag from a previous release as an argument to retrieve the schema from that release.  It only outputs final tables (no intermediary or staging models), and writes input layer models and tuva project models to separate files.

 - usage
   - Latest release: `py schema_extract.py`
   - Previous release: `py schema_extract.py v10.0.1`

### terminology_sql

This folder contains warehouse-specific scripts to load the terminology data sets included in the tuva project to a data warehouse without needing to set up a dbt project.  These scripts are not kept up to date with every release, and may not represent the latest versions of each terminology set.