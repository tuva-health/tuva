{% docs __overview__ %}

# Overview

Tuva transforms your healthcare data so that it's ready for machine learning and analytics.  The current release includes 3 data marts (see table below).  Each data mart is self-contained in its own folder within the models folder.

| **Data Mart** | **Description** |
| ------------- | --------------- |
| Staging | Source-to-target mapping layer - this is where your raw data enters Tuva. |
| Chronic Conditions | Creates 69 patient-level chronic condition flags across 9 clinical areas in wide and long table formats. |
| CCSR Diagnosis Categories | Creates 530 clinical categories (diagnosis groups) across 21 clinical areas in wide and long table formats. |

# Staging

### Description
This data mart is the initial layer you map your source data to.  Currently there are 4 total tables and 19 total columns required to run the entire Tuva proejct.

### Usage
Create each staging table in your data warehouse, using the docs to see exactly how to define each table.  Configure the location of the tables (database name, schema name, and table name) in the dbt_project.yml file.

# Chronic Conditions

### Description
Chronic conditions are an important feature of many different analytics and machine learning use cases.  This data mart calculates chronic condition flags at the patient-level based on code sets and logic from the CMS Chronic Condition Warehosue (CCW).  There are 69 distinct chronic conditions coded in this data mart, grouped into 9 clinical categories.

Both the code sets and logic vary for each condition.  For example, some conditions look for any evidence of a diagnosis code while others required the diagnosis code is primary.  Other conditions have both inclusion and exclusion criteria.

### Usage
This data mart outputs a wide table and long table.  

The wide table (chronic_conditions_wide) contains one record per patient and one column per chronic condition.  For each chronic condition column, the patient will receive a '1' if they have the condition and '0' otherwise.  This table is useful for adding chronic condition features to a patient-level dataset for machine learning.

The long table (chronic_conditions_long) contains one record per patient per condition.  This long format makes it easy to exclude patients that have any of a subset of chronic diseases.  The long table also includes metrics related to each chronic condition, including date of onset, most recent diagnosis date, and total number of encounters the patient has had with the chronic condition.

# CCSR Diagnosis Categories

### Description
CCSR is a diagnosis grouper that makes it easy to analyze diagnosis patterns or create diagnostic features for machine learning.  This data mart calculates CCSR Categories (diagnosis groups) at the encounter-level.  There are 530 CCSR Categories grouped into 21 clinical areas. 

### Usage
This data mart outputs a wide table and a long table.

The wide table (ccsr_dx_wide) contains one record per encounter and one column per CCSR.  

The long table (ccsr_dx_long) contains one record per encounter per CCSR.  

Each record in the long table is flagged as being the default CCSR for inpatient and outpatient.  The default flags are needed because an individual diagnosis code may be grouped into multiple CCSR Categories.  The default flags, which are based on the primary or principle diagnosis code for the encounter, establish the primary CCSR Category for each encounter.

{% enddocs %}