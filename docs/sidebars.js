/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    "welcome",
    "getting-started",
    {
      type: "category",
      label: "Tuva Core Platform",
      className: "sidebar-section",
      collapsible: false,
      collapsed: false,
      items: [
        {
          type: "doc",
          id: "input-layer",
          label: "1. Input Layer",
        },
        {
          type: "category",
          label: "2. Data Quality",
          collapsed: true,
          items: [
            "data-quality-overview",
            "data-pipeline-tests",
            "data-quality-dashboard",
          ],
        },
        {
          type: "doc",
          id: "core-platform/normalization",
          label: "3. Normalization",
        },
        {
          type: "category",
          label: "4. Claims Preprocessing",
          collapsed: true,
          items: [
            {
              type: "doc",
              id: "core-platform/claims-preprocessing",
              label: "Overview",
            },
            {
              type: "doc",
              id: "core-platform/service-categories",
              label: "Service Categories",
            },
            {
              type: "doc",
              id: "core-platform/encounter-grouper",
              label: "Encounter Grouper",
            },
          ],
        },
        {
          type: "doc",
          id: "core-data-model/overview",
          label: "5. Core Data Model",
        },
        {
          type: "category",
          label: "6. Data Marts",
          collapsed: true,
          items: [
            "data-marts/overview",
            "data-marts/ahrq-measures",
            "data-marts/ccsr",
            "data-marts/chronic-conditions",
            "data-marts/cms-hccs",
            "data-marts/ed-classification",
            "data-marts/fhir-preprocessing",
            "data-marts/financial-pmpm",
            "data-marts/hcc-recapture",
            "data-marts/hcc-suspecting",
            "data-marts/pharmacy",
            {
              type: "doc",
              id: "data-marts/tuva-provider-attribution",
              label: "Provider Attribution",
            },
            "data-marts/quality-measures",
            "data-marts/readmissions",
          ],
        },
        {
          type: "doc",
          id: "terminology",
          label: "7. Data Assets",
        },
      ],
    },
    {
      type: "category",
      label: "Additional Tools",
      className: "sidebar-section",
      collapsible: false,
      collapsed: false,
      items: [
        {
          type: "doc",
          id: "predictive-models/risk-adjusted-benchmarking",
          label: "Benchmarking",
        },
        {
          type: "category",
          label: "Connectors",
          collapsed: true,
          items: [
            "connectors/overview",
            "connectors/building-a-connector",
            "connectors/pre-built-connectors",
            "connectors/fhir-inferno",
          ],
        },
        "dashboards",
        {
          type: "category",
          label: "EMPI Lite",
          collapsed: true,
          items: [
            {
              type: "doc",
              id: "empi-lite/index",
              label: "Overview",
            },
            "empi-lite/getting-started",
            "empi-lite/data-requirements",
            "empi-lite/configuration",
            "empi-lite/manual-review",
            "empi-lite/outputs-reference",
            "empi-lite/faq",
          ],
        },
        "notebooks",
      ],
    },
    {
      type: "category",
      label: "Miscellaneous",
      className: "sidebar-section",
      collapsible: false,
      collapsed: false,
      items: [
        {
          type: "category",
          label: "Data Warehouse Support",
          collapsed: true,
          items: [
            "data-warehouse-support/data-warehouse-support-overview",
            "data-warehouse-support/tuva-databricks",
          ],
        },
        "official-tuva-maintainers",
        "vocab-normalization",
        "dbt-variables",
        "example-sql",
      ],
    },
  ],
  knowledgeSidebar: [
    "knowledge/introduction",
    {
      type: "category",
      label: "Getting Started",
      collapsed: true,
      items: [
        "knowledge/getting-started/data-engineering-tools",
        "knowledge/getting-started/data-science-tools",
      ],
    },
    {
      type: "category",
      label: "Part 1: Healthcare Data Sources",
      collapsible: false,
      className: "sidebar-chapter",
      items: [
        "knowledge/data-sources/member-and-enrollment-data",
        {
          type: "category",
          label: "Medical Claims Data",
          collapsible: true,
          collapsed: true,
          items: [
            "knowledge/data-sources/claims-data/intro-to-claims",
            "knowledge/data-sources/claims-data/header-line",
            "knowledge/data-sources/claims-data/key-data-elements",
            "knowledge/data-sources/claims-data/adjustments-denials-reversals",
            "knowledge/data-sources/claims-data/incurred-paid-runout",
          ],
        },
        "knowledge/data-sources/pharmacy-claims-data",
        "knowledge/data-sources/provider-data",
        "knowledge/data-sources/ehr-data",
        "knowledge/data-sources/fhir-apis",
        "knowledge/data-sources/adt-messages",
        "knowledge/data-sources/lab-results",
      ],
    },
    {
      type: "category",
      label: "Part 2: Claims Groupers & Algorithms",
      collapsible: false,
      className: "sidebar-chapter",
      items: [
        "knowledge/claims-groupers-and-algos/service-categories",
        "knowledge/claims-groupers-and-algos/encounters",
        "knowledge/claims-groupers-and-algos/attribution",
      ],
    },
    {
      type: "category",
      label: "Part 3: Analytics",
      collapsible: false,
      className: "sidebar-chapter",
      items: [
        {
          type: "category",
          label: "Cost & Utilization",
          collapsed: true,
          items: [
            "knowledge/analytics/member-months",
            "knowledge/analytics/basic-pmpm",
            "knowledge/analytics/utilization-metrics",
            "knowledge/analytics/ed-visits",
            "knowledge/analytics/acute-ip-visits",
          ],
        },
        "knowledge/analytics/hospital-readmissions",
        "knowledge/analytics/quality-measures",
        "knowledge/analytics/risk-adjustment",
      ],
    },
  ],

  communitySidebar: [
    "community/community-overview",
    "community/community-meetups",
    "community/manifesto",
  ],
};

module.exports = sidebars;
