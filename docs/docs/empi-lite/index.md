---
title: Overview
---

# EMPI Lite Overview

Tuva has two EMPI solutions:

- EMPI Lite: the current primary offering, sold through the Tuva Marketplace
- Legacy EMPI: the earlier open source implementation built on Splink with a frontend layer

Most teams should start with EMPI Lite. It is faster to implement and easier to maintain. A typical setup takes days, while legacy enterprise EMPI projects can take months.

EMPI Lite is a paid product. We still publish the documentation here so implementation details and behavior are fully transparent.

Legacy EMPI resources:

- [Legacy EMPI docs](https://tuva-health.github.io/tuva_empi/docs/)
- [Legacy EMPI code](https://github.com/tuva-health/tuva_empi)

## What EMPI Lite does

EMPI Lite links patient records across source systems and assigns one persistent `empi_id` per real person. It runs as dbt SQL in your warehouse.

Core capabilities:

- Probabilistic matching across demographic attributes
- Configurable blocking, weights, thresholds, and penalties
- Connected component clustering for transitive matching
- Golden record generation for one resolved patient profile per `empi_id`
- Human review queues for borderline matches and split candidates
- Full event history for audit and traceability

## What you get

EMPI Lite is delivered as source code, not a hosted API. You run it in your own dbt environment.

Outputs include:

- `empi_crosswalk` for source id to `empi_id` mapping
- `empi_golden_record` for resolved demographics
- `empi_patient_events` for explainable event level history
- Review queues and supporting anomaly tables

## Tuva integration

EMPI Lite produces Tuva compatible patient and eligibility outputs where `person_id = empi_id`. Point your Tuva input layer to those outputs and downstream marts inherit the resolved identity.

## Start here

- [Getting Started](/empi-lite/getting-started)
- [Data Requirements](/empi-lite/data-requirements)
- [Configuration](/empi-lite/configuration)
