# Tasman Pipeline V2

## Overview
An end-to-end data pipeline that extracts brewery data from the Open Brewery DB API, loads it into DuckDB, and transforms it using dbt to produce a final mart table for analysis.
```
## Project Structure
tasman_pipeline_v2/
├── src/
│   ├── extract.py        # Pulls data from API, saves to bronze/
│   ├── load.py           # Loads bronze JSON into DuckDB silver layer
│   └── transform.py      # Runs dbt models and tests via subprocess
├── breweries_v2/         # dbt project
│   ├── models/
│   │   ├── staging/      # stg_breweries_v2.sql
│   │   └── marts/        # mart_brewery_by_state.sql
├── bronze/               # Raw JSON from API
├── pipeline.py           # Orchestrator - runs all three steps in order
└── README.md
```
## Setup
1. Clone the repo
2. Create and activate a virtual environment: `uv venv` then `source .venv/bin/activate`
3. Install dependencies: `uv add requests "dlt[duckdb]" "dlt[parquet]" dbt-duckdb marimo pandas`
4. Update `~/.dbt/profiles.yml` with the path to your local `breweries_v2.duckdb`

## How to Run
From the root of the project:
python pipeline.py
This will extract, load, and transform the data in one go.

## Tech Stack
- **Python** — extract and load scripts
- **requests** — API calls
- **dlt** — loading data into DuckDB
- **DuckDB** — local analytical database
- **dbt** — data transformation and testing
- **Medallion architecture** — bronze (raw JSON) → silver (DuckDB) → gold (dbt marts)
- **marimo** - interactive notebook for analysis

## Analysis
To explore the data interactively, run:
marimo edit analysis.py
This opens a notebook with three questions answered from the mart table.
