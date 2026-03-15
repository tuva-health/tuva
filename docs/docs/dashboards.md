---
id: dashboards
title: "Dashboards"
hide_title: false
toc: false
---

export const dashboards = [
  {
    title: 'Cost & Utilization Drivers',
    image: '/img/cost_drivers.png',
    href: 'https://app.fabric.microsoft.com/view?r=eyJrIjoiN2Y2YjNkYWEtY2ZkZi00ZGIxLTk4ODMtNmU2ZjMxYjM3YzUyIiwidCI6ImJiM2M3Y2U1LTI2MjgtNGM5MS04Y2VmLTgwMTdjNjExNTk3OCIsImMiOjZ9',
    description:
      'This dashboard demonstrates how you can use the Tuva service category, encounter, and condition groupers to drill into claims data to identify opportunities for improvement.',
  },
  {
    title: 'Data Quality',
    image: '/img/dqi.png',
    href: 'https://app.fabric.microsoft.com/view?r=eyJrIjoiYTFhZWE0OWEtM2NjMi00ZGJlLTgxMjEtNmZmYmFiZmM5ODczIiwidCI6ImJiM2M3Y2U1LTI2MjgtNGM5MS04Y2VmLTgwMTdjNjExNTk3OCIsImMiOjZ9',
    description:
      'This dashboard demonstrates how you can use the Tuva data quality tables to profile and identify atomic-level data quality issues in raw claims data.',
  },
  {
    title: 'Population Insights',
    image: '/img/pop_insights.png',
    href: 'https://app.fabric.microsoft.com/view?r=eyJrIjoiODc2MjNkMWQtNzFlOC00Njg1LWJiNTEtYTU3MTE1NDJlNDhmIiwidCI6ImJiM2M3Y2U1LTI2MjgtNGM5MS04Y2VmLTgwMTdjNjExNTk3OCIsImMiOjZ9',
    description:
      'This dashboard demonstrates how several data marts from Tuva can be used to profile your patient population looking at demographics, chronic disease burden, spend, utilization, and acute events.',
  },
  {
    title: 'Quality Measures',
    image: '/img/quality_measures.png',
    href: 'https://app.fabric.microsoft.com/view?r=eyJrIjoiODdjMWI1NDEtZGMxYi00ZjIwLTgyNzctYTI0YTQzMzhkZjhiIiwidCI6ImJiM2M3Y2U1LTI2MjgtNGM5MS04Y2VmLTgwMTdjNjExNTk3OCIsImMiOjZ9',
    description:
      'This dashboard demonstrates how you can use the quality measures data mart to analyze clinical quality measures and care gaps.',
  },
  {
    title: 'Risk-adjusted Benchmarks',
    image: '/img/risk_adjusted_benchmarks.png',
    href: 'https://app.fabric.microsoft.com/view?r=eyJrIjoiMDY2OWY2ZjEtYjFmYS00ZmU0LTk1YTItMTA1ODEyMjY4MWJmIiwidCI6ImJiM2M3Y2U1LTI2MjgtNGM5MS04Y2VmLTgwMTdjNjExNTk3OCIsImMiOjZ9',
    description:
      'This dashboard shows the expected values produced by the benchmarking mart and compares them to actual values for analysis.',
  },
];

Dashboards run automatically on top of the Core Data Model and Data Marts. The dashboards below are hosted by Tuva and run on synthetic datasets. The code for these dashboards can be found [here](https://github.com/tuva-health/analytics_gallery).

<div style={{ display: 'flex', flexDirection: 'column', gap: '50px' }}>
  {dashboards.map((dashboard) => (
    <a
      key={dashboard.title}
      href={dashboard.href}
      target="_blank"
      rel="noopener noreferrer"
      style={{
        display: 'flex',
        alignItems: 'flex-start',
        color: 'inherit',
        textDecoration: 'none',
      }}
    >
      <img
        src={dashboard.image}
        alt={`${dashboard.title} thumbnail`}
        style={{
          width: '120px',
          height: 'auto',
          marginRight: '30px',
          flexShrink: 0,
        }}
      />

      <div>
        <h3 style={{ margin: '0 20px' }}>{dashboard.title}</h3>
        <div style={{ margin: '5px 20px', fontSize: '0.9em', lineHeight: '1.4em' }}>
          {dashboard.description}
        </div>
      </div>
    </a>
  ))}
</div>
