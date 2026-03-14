---
title: Understanding Medicare ACO Assignment
subtitle: Why CCLFs Are Failing Your ACO (and How to Fix it with ALRs)
description: In Medicare ACOs member assignment is complex -- but it doesn't have to be.
image: /img/aco-meme.jpg
tags: [tuva, dbt, aco]
authors:
  - name: Mike Krahulec
    title: Co-Founder and Principal Consultant
    url: https://www.linkedin.com/in/mike-krahulec-62867410
date: 2026-02-19
toc_min_heading_level: 2
toc_max_heading_level: 2
---

**_[Mike Krahulec](https://www.linkedin.com/in/mike-krahulec-62867410/) is Co-Founder and Principal Consultant at Z2 Health Insights, a healthcare data & analytics consulting firm. Previously he led data and analytics at multiple healthcare organizations including TailorCare and Bright Health. This work was funded in part by UPenn via a generous philanthropic gift._**

If you are involved in the data operations of a Medicare Shared Savings Program (MSSP) ACO, you live in a world of zip files, CSVs, and Excel docs. You’re constantly working to combine information across disparate data sets to understand the performance of your assigned beneficiaries. Unfortunately, each of these data sources and reports was originally created for slightly different use cases, and combining them is challenging and requires a thorough understanding of each file set and its limitations.

Many organizations focus heavily on designing logic to determine how best to manage their population and which clinical interventions are most appropriate for each assigned patient at a given time. But there is a more fundamental problem that plagues many ACOs: knowing exactly who is in their assigned population at any given moment.

If you rely primarily on your Claim and Claim Line Feed (CCLF) files to determine your patient roster, your reporting on your assigned beneficiaries, and therefore your ACO's performance, is likely wrong. To get an accurate view of your assigned beneficiaries, you need to leverage the Assignment List Reports (ALRs).

Today, we’re diving deep into the mechanics of ALRs, why using CCLFs alone creates unintended blind spots & inaccuracies, and how the Tuva Project is solving this challenge with our newest connector.

## The Basics: Why You Get ALRs

In the MSSP, "assignment" is everything. It determines the specific population of Medicare fee-for-service beneficiaries for whom your ACO is responsible. CMS assigns beneficiaries to your ACO based on where they receive the plurality of their primary care services. This is communicated to ACOs via the Assignment List Reports (ALRs).

If you are in a track with "Prospective Assignment," at the beginning of each year CMS essentially says, "Here is the list of patients we predict will belong to you for the coming performance year based on historical data." You now know your patient list for the year, and this list will not change beyond some members being disqualified due to specific enrollment events. 

However, patients may stop seeing your physicians during the year, and you’ll still be held accountable for those patient’s performance. Because of this, most ACOs instead choose to use "Preliminary Prospective Assignment with Retrospective Reconciliation", and while this provides a more accurate list of patients assigned to your ACO during the performance year, it complicates tracking which patients are actually assigned to your ACO.

## Preliminary Prospective Assignment – I Hope You Enjoy Retroactivity!

First, let’s restate the obvious: if you are using preliminary prospective assignment for your ACO population, your patient list is not static. It is a living, breathing roster that changes throughout the year. CMS provides several different flavors of ALRs to keep you updated:

1. Initial Annual ALR: The starting roster for the performance year.
2. Benchmark ALRs: Historical files used to understand your patient’s historical enrollment and utilization.
3. Quarterly ALRs: Updates provided throughout the year as your roster changes.

Here is the crucial challenge: Every time you receive a Quarterly ALR, CMS effectively restates your assigned patients for the entire performance year (a restatement referred to in the healthcare industry as “retroactivity”). Each Quarterly ALR is a snapshot in time, with the most recent QALR report showing who CMS would currently assign to your ACO based on the most recent claims look back period.

However, note that although you’ll frequently hear the ALR report referred to as a single entity, an ALR report is NOT a single data file; instead, it’s a set of six separate files that need to be combined. The 6 disparate reports need to be preprocessed and combined to create the unified view we’d like to see when tracking a patient, their demographics, and the provider, TIN and CCN responsible for their care.

Additionally, no single iteration of the ALR report provides you with a full history. Want to know why a patient was assigned in Q1, dropped off in Q2, and was re-assigned in Q3? You must longitudinally stitch together the Initial ALR and every subsequent Quarterly ALR to build a month-by-month eligibility history.

Without this complex historical combination, it’s very difficult to understand why your patient population has changed and to accurately count beneficiary member months and assigned person years (the denominators for many MSSP ACO financial performance calculations). And if your denominators are wrong, you may be misinterpreting your ACO's financial performance and be in for a surprise when you receive CMS’s final financial reconciliation reports with your shared savings (or lack thereof).

## The Problem: Why Can’t I Just Use CCLFs?

![Extension Column Pass-Through Architecture](/img/aco-meme.jpg)

Many ACOs try to bypass the complexity of ALRs by relying on their monthly Claim and Claim Line Feed (CCLF) files to define their population. Most organizations do this because the CCLF files (or their BCDA replacement) are received in a more easily processed format on a regular cadence (monthly). Unfortunately, this is a dangerous approach that leads to significant inaccuracies for two primary reasons.

**1. Overstating Your Population**
CCLF files are designed to give you claims data for patients cared for by your ACO providers. However, CCLF files often contain records for beneficiaries who have had a visit with one of your TINs but are not actually assigned to your ACO.
If a Medicare patient from another state gets sick while vacationing near your clinic and sees your doctor, their claims might show up in your CCLF feed. If you count them in your population, you are artificially inflating your roster with patients whose total costs you aren't responsible for.

**2. Beneficiary Opt-outs**
Medicare beneficiaries have the right to opt out of having their claims data shared with ACOs.  If an assigned beneficiary opts out of data sharing, they will completely disappear from your CCLF files. You will receive zero claims records for them.  However, they are still assigned to your ACO via the ALR.

You are still financially responsible for their care, and their outcomes still impact your quality scores, but you have absolutely no visibility into their utilization via claims. If you rely only on CCLFs, these patients are invisible to your analytics team, making it impossible to track them or perform necessary outreach. Only the ALR reveals their existence.


## The Solution: An ALR history

To accurately report cost and utilization performance, an ACO must move beyond using static lists.

You need to construct a longitudinal "beneficiary history." This requires a sophisticated data processing workflow that ingests benchmarks, initial and quarterly ALRs, stacks them chronologically, prioritizes the latest data received for a particular enrollment period, and determines eligibility flags for every month of the performance year for each unique Medicare ID.

Only once you have this accurate eligibility can we join claims data received via CCLFs to create a (mostly) accurate understanding of the performance of individual beneficiaries (although you’ll still need to account for claim-level exclusions for Behavioral Health & Substance Abuse).

## Introducing the Tuva ALR Connector

Building this longitudinal ALR processing engine from scratch is tedious, complex, and error-prone.  That’s why we are excited to announce a [new connector](https://github.com/tuva-health/cms_alr_connector) in the Tuva Project specifically designed for MSSP ALR files.

This new connector handles the heavy lifting of combining the various ALR formats, processing & combining the quarterly ALRs, and generating an accurate, month-by-month eligibility record for your assigned population.

Crucially, this new ALR connector is designed to integrate seamlessly with the existing [Tuva CCLF connector](https://github.com/tuva-health/medicare_cclf_connector).

By combining these two tools, Tuva now allows ACOs to:
- Instantly see which assigned members are missing from claims data because they opted out.
- Filter out claims for patients who visited your providers but aren't assigned to your ACO.
- Have confidence in your financial denominators by using a true, longitudinal view of monthly enrollment.
