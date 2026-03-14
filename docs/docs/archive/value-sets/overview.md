---
id: overview
title: "Overview"
hide_title: true
---

# 6. Value Sets
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-21-2025</em></small>
</div>

Value sets are lookup tables that define concepts that are useful for analytics.  For example: conditions, therapies, service categories, etc.  Many data marts leverage value sets (e.g. quality measures, HCCs, chronic conditions, etc).

Like Terminology Sets, Value Sets are stored on S3 because they are too large for GitHub.

<table>
  <thead>
    <tr>
      <th>Value Set</th>
      <th>Source</th>
      <th>Data Mart</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="../value-sets/ahrq-measures">AHRQ Measures</a></td>
      <td><a href="https://qualityindicators.ahrq.gov/">AHRQ Website</a></td>
      <td><a href="../data-marts/ahrq-measures">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/ccsr-groupers">CCSR Groupers</a></td>
      <td><a href="https://hcup-us.ahrq.gov/toolssoftware/ccsr/ccs_refined.jsp">HCUP Website</a></td>
      <td><a href="../data-marts/ccsr">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/chronic-conditions">Chronic Conditions</a></td>
      <td><a href="https://www2.ccwdata.org/web/guest/home/">CMS</a></td>
      <td><a href="../data-marts/chronic-conditions">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/clinical-concepts">Clinical Concepts</a></td>
      <td>Tuva</td>
      <td><a></a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/cms-hccs">CMS-HCCs</a></td>
      <td><a href="https://www.cms.gov/medicare/payment/medicare-advantage-rates-statistics/risk-adjustment">CMS</a></td>
      <td><a href="../data-marts/cms-hccs">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/data-quality">Data Quality</a></td>
      <td>Tuva</td>
      <td><a></a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/ed-classification">ED Classification</a></td>
      <td><a href="https://wagner.nyu.edu/faculty/billings/nyued-background">NYU ED Algorithm</a></td>
      <td><a href="../data-marts/ed-classification">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/hcc-suspecting">HCC Suspecting</a></td>
      <td>Tuva</td>
      <td><a href="../data-marts/hcc-suspecting">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/quality-measures">Quality Measures</a></td>
      <td>Various</td>
      <td><a href="../data-marts/quality-measures">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/readmissions">Readmissions</a></td>
      <td><a href="https://www.cms.gov/medicare/quality/initiatives/hospital-quality-initiative/measure-methodology">CMS</a></td>
      <td><a href="../data-marts/readmissions">Link</a></td>
    </tr>
    <tr>
      <td><a href="../value-sets/service-categories">Service Categories</a></td>
      <td>Tuva</td>
      <td><a></a></td>
    </tr>
  </tbody>
</table>