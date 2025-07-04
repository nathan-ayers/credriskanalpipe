Local Financial Data Pipeline

An end-to-end, zero‑cost project showcasing data engineering, analytics, and modeling on financial data.

Project Overview:

This pipeline ingests raw financial datasets (bank stock prices, loan data, macro indicators), transforms and stores them locally in DuckDB, runs analytics and a basic credit‑risk model, and serves results via an interactive Streamlit dashboard. Everything runs on your laptop with no cloud costs.

Value & Industry Relevance

Mirrors real‑world banking workflows: credit risk, market analysis, macro correlations.

Demonstrates modern data‑ops best practices: Airflow/cron scheduling, dbt models, CI/CD tests.

Highlights skills in SQL, Python, dbt, Docker, and visualization tools.

Key Features:

Data Engineering: Automated ingestion of daily stock prices, Lending Club loans CSVs, and FRED macro data.

Local Storage: DuckDB as a self‑contained warehouse; Parquet staging for performance.

Transformations: dbt models with tests, docs, and a star schema (dimensions & facts).

Analytics: SQL and Python scripts computing volatility, Sharpe ratios, default rates, and macro correlations.

Modeling: Baseline logistic regression for default prediction with performance metrics (ROC/AUC).

Dashboard: Streamlit app with KPI cards, interactive time‑series, drill‑downs, and model threshold explorer.

Automation: Airflow (Docker) or Cron + Makefile schedules every step end-to-end.

CI/CD: GitHub Actions to run dbt tests and Python unit tests on every push.

Architecture Diagram

 ┌───────────┐     fetch & parquetize    ┌───────────┐
 │ Raw Data  │ ────────────────────────▶ │ staging/  │
 │(CSV, API) │                           │(Parquet)  │
 └───────────┘                           └───────────┘
      │                                      │
      │                                      │
      ▼                                      ▼
 ┌───────────┐      dbt models & tests    ┌─────────────────┐
 │ DuckDB    │ ◀────────────────────────  │ dbt/            │
 │ warehouse │                            │ models/         │
 └───────────┘                            └─────────────────┘
      │                                      │
      │ Python & SQL analytics              │
      ▼                                      │
 ┌───────────┐      Streamlit UI     ┌─────────────────┐
 │ Notebooks │ ────────────────────▶ │ streamlit_app/  │
 └───────────┘                       └─────────────────┘
      ▲                                      │
      │ CI/CD (GitHub Actions)               │
      └──────────────────────────────────────┘

Repository Structure

project/
├─ data/
│  ├─ raw/            # downloaded CSVs & JSON
│  ├─ staging/        # Parquet files
│  └─ warehouse.duckdb
├─ airflow/           # Docker Compose for Airflow DAGs
├─ dbt/               # dbt project (models, tests, docs)
├─ notebooks/         # EDA & modeling notebooks
├─ streamlit_app/     # Streamlit dashboard code
├─ scripts/           # ingestion & parquetize scripts
├─ docker-compose.yml # Airflow, DuckDB, MinIO/Postgres optional
├─ Makefile           # cron-friendly tasks
└─ README.md          # this file

What each folder/file is for
Path	Purpose
data/raw/	Store the original, untouched CSVs and JSON files you download each day. You never edit these—so you always have a “ground truth” copy to fall back on if something breaks.
data/staging/	After cleaning/parquetizing, your scripts write to here. Parquet files live here for super-fast reads by DuckDB or other tools.
scripts/	Python (or bash) ingestion & cleaning scripts. For example:

fetch_raw.py: downloads Lending Club, yfinance, FRED data

parquetize.py: converts raw CSV → Parquet, applies simple cleaning rules |
| dbt/models/ | All your SQL transformation logic lives here. dbt will pick up .sql files and build your star schema (dimensions & facts). You’ll also add schema.yml tests in the parent dbt/. |
| notebooks/ | Interactive Jupyter notebooks for EDA, feature engineering, and building your baseline credit-risk model in Python. |
| streamlit_app/ | The code for your Streamlit dashboard: layouts, charts, KPI cards, and the model threshold explorer. |
| airflow/ | Docker Compose files and DAG definitions if you choose Airflow. This lives entirely locally, so you can run docker-compose up and watch your pipeline run. |
| tests/ | Unit tests for your Python code (e.g. data-validation functions) and any dbt test configurations beyond the auto-generated ones. |
| docs/ | Any hand-drawn diagrams (e.g. PNG/SVG architecture diagram), export of dbt docs, or extra markdown guides for modules. |
| .gitignore | Lists files/folders Git should ignore (e.g. data/raw/, virtual environments, *.pyc). |
| Makefile | Defines shortcuts like make ingest, make transform, and make dashboard so you can run the whole pipeline with a single command (and cron later). |
| README.md | The high-level overview you just created. |