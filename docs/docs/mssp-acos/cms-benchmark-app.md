---
id: cms-benchmark-app
title: "CMS Benchmark App"
hide_title: true
---

# CMS Benchmark App

The CMS Benchmark App (`cms_benchmark_app_v2`) is a Flask web application that calculates an MSSP ACO's projected savings against the CMS Minimum Savings Rate (MSR). It implements the 23-step benchmark calculation methodology defined by CMS in the "Updated Benchmark Estimated Calculation" reference workbook.

## Background: The MSR and Benchmark Calculation

Each year, CMS calculates a benchmark expenditure for an ACO — the expected per-capita cost for their assigned beneficiaries based on historical spending and national trends. The ACO generates shared savings if its actual expenditures fall below the benchmark by at least the MSR threshold.

The MSR varies based on:

- **Variable MSR** — For smaller ACOs; determined by beneficiary count via interpolation (ranges from ~3.9% for the smallest ACOs down to 2.0% for larger ones)
- **Fixed MSR** — For larger two-sided ACOs (≥5,000 beneficiaries); options are 0.0%, 0.5%, 1.0%, 1.5%, or 2.0%

The app implements the full 23-step calculation using data from three CMS-provided Excel reports.

## Required Input Files

You will need three Excel files available through the CMS ACO portal:

| File | CMS Name | What It Contains |
|---|---|---|
| **BNMRK** | Historical Benchmark | Table 1: historical benchmark expenditures [L] by enrollment type; Table 6: ACPT values [J] by enrollment type |
| **BY3 EXPU** | National Assignable FFS | National assignable FFS per capita [A] by enrollment type |
| **QEXPU** | Quarterly Expenditure & Utilization | Quarterly expenditure and utilization data including [B], [N], [Q], [S], [D], [E], [G], [H] |

The app auto-detects the latest available quarter in the QEXPU file, so you do not need to specify it manually.

## Enrollment Types

The calculation is broken out across four Medicare enrollment categories:

| Category | Description |
|---|---|
| ESRD | End-Stage Renal Disease beneficiaries |
| Disabled | Non-aged disabled beneficiaries |
| Aged/Dual | Aged beneficiaries with Medicaid dual eligibility |
| Aged/Non-Dual | Aged beneficiaries without dual eligibility |

## Installation

```bash
cd cms_benchmark_app_v2
pip install -r requirements.txt
```

## Running the App

```bash
python app.py
```

The app starts in debug mode on port 5000. Navigate to `http://localhost:5000` in your browser.

## Usage

1. **Select Performance Year** — Choose PY1 through PY5 (the performance year within your agreement period)

2. **Select MSR Type** — Choose one of:
   - Variable (default for one-sided models and smaller ACOs)
   - Fixed: 0.0%, 0.5%, 1.0%, 1.5%, or 2.0%

3. **Upload files** — Upload the BNMRK, BY3 EXPU, and QEXPU Excel files using the file pickers

4. **Click Calculate** — The app parses the files and runs the calculation

5. **Review results** — The results page shows extracted inputs by enrollment type, all 23 calculated values, projected savings, and MSR comparison

## The 23-Step Calculation

The app implements variables [A] through [W] as defined by CMS:

| Variable | Description |
|---|---|
| [A] | National assignable FFS per capita (from BY3 EXPU) |
| [B] | ACO per capita expenditure for the performance period |
| [C] | Ratio [B]/[A] — ACO expenditure relative to national |
| [D] | National FFS per capita for the update period |
| [E] | ACO expenditure for the update period |
| [F] | Ratio [E]/[D] |
| [G] | Weight for the performance period ratio |
| [H] | Weight for the update period ratio |
| [I] | Blended ratio: ([C]×[G]) + ([F]×[H]) |
| [J] | ACPT value (from BNMRK Table 6) |
| [K] | Updated benchmark factor: (2/3)×[I] + (1/3)×[J] |
| [L] | Historical benchmark expenditure (from BNMRK Table 1) |
| [M] | Updated per capita benchmark: [K]×[L] |
| [N] | Assigned beneficiary count for the performance period |
| [O] | Enrollment type weight |
| [P] | Aggregate benchmark: SUMPRODUCT([O],[M]) |
| [Q] | ACO total expenditure for the performance period |
| [R] | Savings rate: ([P]-[Q])/[P] |
| [S] | Assigned beneficiary count for MSR calculation |
| [T] | MSR threshold |
| [U] | Savings above MSR |
| [V] | Sharing rate |
| [W] | Estimated shared savings payment |

## Results Interpretation

The results page shows:

- **Projected savings rate [R]** — How much lower ACO expenditure is compared to the benchmark, as a percentage
- **MSR threshold [T]** — The minimum savings rate required to earn shared savings
- **Estimated savings payment [W]** — The projected dollar amount of shared savings, if savings exceed the MSR

If the savings rate [R] exceeds the MSR [T], the ACO is projected to earn shared savings for the performance year.

## Project Structure

```
cms_benchmark_app_v2/
├── app.py                     # Flask app: routes, filters, startup
├── calculations/
│   ├── parser_bnmrk.py        # Extracts [L] and [J] from BNMRK Excel
│   ├── parser_expu.py         # Extracts [A] from BY3 EXPU Excel
│   ├── parser_qexpu.py        # Extracts [B],[N],[Q],[S],[D],[E],[G],[H] from QEXPU Excel
│   ├── parser_common.py       # Shared label-matching utilities
│   ├── calculator.py          # 23-step calculation logic (steps A–W)
│   └── msr_lookup.py          # MSR interpolation for variable MSR
├── templates/
│   ├── index.html             # Upload form
│   └── results.html           # Results display
└── requirements.txt
```

## Error Handling

If the app cannot locate expected data in an uploaded file (due to a layout change or wrong file selection), it will display a descriptive `ParseError` message identifying which value it could not find. This helps diagnose mismatched file types or unexpected CMS format changes.
