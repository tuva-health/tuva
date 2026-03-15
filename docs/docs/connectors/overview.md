---
id: overview
title: "Overview"
hide_title: true
# toc_hide: true
---

# 1. Connectors

A connector is a dbt project that contains SQL-based logic for transforming raw healthcare data sources (e.g. claims data, EHR data) into the Tuva [Input Layer](../input-layer).  The Tuva package expects data to be in the Input Layer format.  Once your data is transformed into the Input Layer format, you can run the entire Tuva package on your data with a single command.

![Connectors](/img/Connectors.jpg)

The video below describes more about connectors.  For more details on building a custom connector to map your own data to the Tuva Project Input Layer, visit the next section on [Building a Connector](/docs/connectors/building-a-connector.md). 

The Tuva Project also has a library of **Pre-Built Connectors** that you can use to map common claims or clinical data sources to the Tuva Input Layer. 

<iframe width="600" height="400" src="https://www.youtube.com/embed/dxH_qWgCoik?si=XB5D_-2p82IaJo8R" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

