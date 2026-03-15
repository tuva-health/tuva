---
id: manifesto
title: "Manifesto"
hide_title: false
---

_Last Updated Aug 18, 2025_

**_In healthcare, we spend 99% of our time cleaning and transforming data and almost zero time actually generating insights. The Tuva Project is our attempt to fix this -- Tuva or Bust._**

Healthcare data has amazing potential in terms of what we can learn from it.  For example, it's possible to use healthcare data to answer questions like:

- How common are adverse effects from new drugs like GLP-1s? 
- Which doctors are the best at managing complex cardiovascular disease? 
- How does cost and quality vary across different providers and geographic locations?

However, analyzing raw healthcare data requires first transforming it into analytics-ready data.  This includes:
- **Normalization:** Standardizing the format and the vocabulary of the data so that it's consistent across data sources.
- **Enrichment:** Adding new tables and columns to the data (e.g. service categories, encounter types, treatment periods, quality measures).
- **Data Quality Testing:** Testing the data to identify data quality problems that will impact analytic results.

Data transformation is necessary because population-scale healthcare data such as claims and medical records are stored in different formats, with tons of data quality problems, and completely lack higher-level concepts that analysts need to answer important questions like those listed above.

Doing data transformation requires a common set of tools.  Almost all data teams we've ever worked with don't have access to all of these tools, and as a result spend enormous time building one or more of these tools:
- **Common Data Model:** Most organizations deal with multiple data sources and need a common data model to standardize the schema so the data format is consistent and data analysis code is reusable across data sources.
- **Connectors:** Lots of time is wasted transforming raw EHR and claims data sources into the common data model.  For example, pretty much every EHR and claims dataset has its own format.  Connectors are the code for transforming these raw sources into a common data model.
- **Terminology:** There are 50+ terminology and reference sets that are needed for healthcare analytics and they are spread all over the internet, maintained by different organizations, and updated on different frequencies.  Teams spend enormous time getting these code sets into their data warehouse and maintaining them over time.
- **Data Marts:** Data analysts need high-level concepts computed on top of the raw data e.g. was this an acute inpatient visit, was it a readmission, does the patient have type 2 diabetes, etc.  Without these they can't answer interesting questions from the data.  The code for creating these concepts is commonly created in what is called a data mart.  
- **Data Quality Testing:** There are numerous common data quality issues in raw healthcare data and the library of tests needed to catch them is common as well.  These tests need to be built into the data pipelines for transforming the raw data into the common data model and data marts.

As an industry we spend 99% of our time building these tools and others, and using them to transform data.  As a result, there is hardly any time actually being spent analyzing data.  

The goal of the Tuva Project is to fix this.  We're open-sourcing all the tools for transforming raw healthcare data into analytics-ready data.  And we're building these tools using modern data technologies that data teams know and love.  We want to make it trivial to transform raw healthcare data into analytics-ready data, so data teams can actually start answering interesting questions.

Our vision is that the entire world uses Tuva to transform their data into the exact same analytics-ready format.  Not only will we dramatically reduce the amount of time and energy spent transforming data, but every analysis will be completely reusable.  In the future anyone can publish a new analysis and that code will be completely reusable by anyone else who has their data in the Tuva data model.

As the amount of time and energy we spend transforming data decreases, and the amount of reusable analyses increase, we think this will result in a dramatic (e.g. 1000x) increase in the amount of high-quality insights being generated from healthcare data.  And as data analysts and patients, this is an enormously exciting future that we're very motivated to work on and be part of.

## Why the Name Tuva?

We are massive [Richard Feynman](https://en.wikipedia.org/wiki/Richard_Feynman) fans.  Feynman embodied so many great traits that are critical for deeply understanding a subject and doing science.

Tuva is an allegory for Feynman.  Tuva was formerly a country in the Soviet Union. For more than a decade before his death, Feynman and his friend [Ralph Leighton](https://en.wikipedia.org/wiki/Ralph_Leighton) tried to travel to the country of Tuva.  What started as a joke became a mission - and it was challenging.  Getting a visa to Soviet Russia during the cold war was next to impossible. Ultimately Feynman died a few weeks before their visas came, but Ralph traveled to Tuva and chronicled the trip and their adventure trying to get there in his book [Tuva or Bust](https://www.amazon.com/Tuva-Bust-Richard-Feynmans-Journey/dp/0393320693).

Ralph helped pen a number of other books about Feynman which we highly recommend.  If you're new to Feynman start with [Surely You're Joking, Mr. Feynman](https://www.amazon.com/Surely-Feynman-Adventures-Curious-Character/dp/0393316041).

The Tuva Project is our attempt to fix healthcare data -- Tuva or Bust.

