[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.9.x&color=orange)

![diagram](./the-tuva-project-3.jpg)

## What is the Tuva Project?
The Tuva Project is a collection of tools for transforming raw healthcare data into analytics-ready data.  The main Tuva package (i.e. this repo) is a dbt package that includes the following components:
- Input Layer
- Claims Preprocessing
- Core Data Model
- Data Marts
- Terminology & Value Sets

Detailed documentation of this package and related tools, including data dictionaries, can be found at www.thetuvaproject.com.

## Agentic Workflow

We are increasingly using agents to use and further develop this package.  You can find context for agents in [agent.md](agent.md).

## Contributing

This is the recommended setup for development:
- Python 3.10 or later
- duckdb
- dbt (dbt-core and dbt-duckdb)

Connect duckdb and dbt via your profile.yml.

Use tuva/integration_tests as your development project.  Configure the dbt_project.yml in this folder to connect to duckdb.

Run the package from integration_tests.  This will:
- Load dev data from seed files stored in the project
- Build the entire pipeline in your duckdb instance

From there we recommend iterating with your preferred coding agent using [agent.md](agent.md).

Hello and welcome! Thank you so much for taking the time to contribute to the Tuva Project. People like you are helping to build a community of healthcare data practitioners that shares knowledge and tools. Whether it’s fixing a bug, submitting an idea, updating the docs, or sharing your healthcare knowledge, you can make an impact!

In this guide, you will get an overview of the contribution workflow, from how to contribute, setting up your development environment, testing, and creating a pull request.
