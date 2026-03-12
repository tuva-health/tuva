---
id: getting-started
title: "Getting Started"
hide_title: true
description: Instructions for getting started with the Tuva Project.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 🏁 Getting Started

To run Tuva you need to do the following:

1. Load your healthcare data (e.g. claims, EHR) into a data warehouse (e.g. Snowflake, Databricks)
2. Install [dbt](https://docs.getdbt.com/docs/core/installation-overview) -- a free open-source tool for transforming data inside your data warehouse
3. Create a new dbt project and connect that project to your data warehouse
4. Map your raw healthcare data to the Tuva [Input Layer](input-layer)
5. Import the Tuva package into your dbt project
6. Run the entire dbt project (i.e. execute "dbt build")

Below we describe how to do this in more detail.  However, if you don't have access to healthcare data or you just want to play around with the Tuva data tables, run the [demo](https://github.com/tuva-health/demo) project (which uses synthetic data) as described in the video below (note this video is slightly out-dated but it should get you there).

<iframe width="560" height="315" src="https://www.youtube.com/embed/C6A1rxkqe_A?si=Rl74kyq9xhPiiVGL" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

## 1. Pre-requisites

In order to run Tuva you need healthcare data loaded into a data warehouse and dbt installed.  You'll need to consult documentation from your data warehouse and from dbt to do these things.

dbt is easy to install using any package manager like pip or homebrew.  

Ensure you're working with a data warehouse that Tuva supports. We officially and unofficially support several data warehouses and you can find the latest up to date info on our data warehouse support page.

## 2. Create a dbt Project

The very first step is to setup a new dbt project.  Once dbt is installed you can do this by executing ```dbt init <project_name>``` where you replace ```<project_name>``` with the name of your project.

Next you need to configure your ```dbt_project.yml``` file in the newly created dbt project.  This includes 3 steps:

- Setting your ```profile.yml```
- Setting Tuva-specific variables
- Setting the database and schema where dbt should write data to

Setting your ```profile.yml``` is how you connect your dbt project to your data warehouse.  dbt has instructions for how to do this which you can find on their docs site.

Next, there are a few dbt variables that you'll need to set which are specific to Tuva.  In your dbt_project.yml, if you have only claims data you need to set `claims_enabled = true` and if you have clinical data you need to set `clinical_enabled = true`.  Add these variables to your dbt_project.yml file.

Next, you'll want to add a "generate schema" macro to your macros folder in the dbt project.  This step is optional, but if you don't do this your schema names will all be prefixed with your default schema e.g. "public_" which is typically annoying.  dbt has documentation on how to do this.

## 3. Map Your Raw Data

The next step is mapping your data to the [Tuva Input Layer](input-layer).  Every healthcare dataset comes in its own schema (i.e. set of tables and columns).  Before you can use Tuva you need to convert your schema to the Tuva Input Layer.  We call this "mapping".  Do this by creating models (i.e. SQL files) in your dbt project to transform your data into the Input Layer format.

Check out the Input Layer data dictionaries for advice on mapping specific tables and columns.  

## 4. Import the Tuva Package

Finally you'll need to import the Tuva dbt package by creating a ```packages.yml``` file inside your dbt project and adding the following code:

```yml
packages:
- package: tuva-health/the_tuva_project
  version: [">=0.12.0","<0.16.0"]
- package: dbt-labs/dbt_utils
  version: [ ">=0.9.2" ]
```

Then execute ```dbt deps``` from the command line to import the package.  This will create a new folder called dbt_packages and these packages will have been loaded into it.

## 5. Execute dbt Build

Next, run ```dbt build``` from the command line to build the entire project.  This will create thousands of data tables and views in your data warehouse.  Your source data will be transformed into the [Core Data Model](../core-data-model), all [Data Marts](../data-marts/overview) will be built, and all [Terminology](../terminology) datasets will be loaded into your data warehouse.  This is pretty cool to see with a single command!

## 6. Explore Data and Docs

At this point you have now transformed you data into the Tuva data model and are ready to do data analysis!  Check out the [Example SQL](example-sql) or [Dashboards](dashboards) pages to see examples of the types of analytics you can do out of the box on your data.  





