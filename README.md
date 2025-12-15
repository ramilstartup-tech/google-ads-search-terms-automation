# Google Ads Search Terms Labeler (Python)

Standalone Python script that labels/classifies Google Ads search terms using a custom Excel-based library.

This tool is built to replace manual search term cleaning and speed up routine PPC work.

## What it does

- Loads a Google Ads search terms export (Excel)
- Loads a keyword/label library (Excel)
- Cleans and normalizes queries
- Assigns a label based on keyword matches and priority rules
- Generates a labeled Excel output for analysis and reporting

## Typical use case

When you need to process thousands of search terms quickly (e.g., 5k–10k+) and:
- classify terms by intent/category
- find irrelevant queries
- build a workflow for negative keyword actions
- prepare data for reporting / BI dashboards

## Requirements

- Python 3.9+
- pandas
- openpyxl

## How to run

1) Install dependencies:
```bash
pip install -r requirements.txt

Before running the script, update file paths in the CONFIG section of `SQ_Labels.py`.
