---
id: overview
title: "Overview"
hide_title: true
hide_table_of_contents: true
slug: /core-data-model
---

# 2. Core Data Model

import InputLayerDictionaryTable from '@site/src/components/InputLayerDictionaryTable';

export const CORE_DATA_MODEL_TABLE_OPTIONS = [
  {
    groupLabel: 'Core Data Model',
    label: 'appointment',
    modelName: 'core__appointment',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'condition',
    modelName: 'core__condition',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'eligibility',
    modelName: 'core__eligibility',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'encounter',
    modelName: 'core__encounter',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'immunization',
    modelName: 'core__immunization',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'lab_result',
    modelName: 'core__lab_result',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'location',
    modelName: 'core__location',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'medical_claim',
    modelName: 'core__medical_claim',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'medication',
    modelName: 'core__medication',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'member_months',
    modelName: 'core__member_months',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'observation',
    modelName: 'core__observation',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'patient',
    modelName: 'core__patient',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'person_id_crosswalk',
    modelName: 'core__person_id_crosswalk',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'pharmacy_claim',
    modelName: 'core__pharmacy_claim',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'practitioner',
    modelName: 'core__practitioner',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'Core Data Model',
    label: 'procedure',
    modelName: 'core__procedure',
    yamlPath: 'models/core/core_models.yml',
  },
];

The Core Data Model is a common data model designed for unifying claims and clinical data into a single longitudinal patient record.  A common data model creates a single source of truth that analytics can be built on top of.  With a common data model all your data sources are in a common format (i.e. standard set of data tables).  This makes it possible for every data person in an organization to share a common language and approach for how they talk about and do analytics.  It also creates a standard layer that downstream algorithms (e.g. data marts, machine learning models) can be built on.

<InputLayerDictionaryTable
  tableOptions={CORE_DATA_MODEL_TABLE_OPTIONS}
  defaultModelName="core__appointment"
  showMappingInstructions={false}
  showRequiredForDataMart={false}
/>


