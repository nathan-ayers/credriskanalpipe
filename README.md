# Local Financial Data Pipeline

An end-to-end, zero‑cost project showcasing data engineering, analytics, and modeling on financial data.

---

## Project Summary

This Local Financial Data Pipeline acquires three core datasets: detailed loan-level records (loan amounts, terms, grades, borrower profiles) from Lending Club; daily bank stock prices (OHLC) for major institutions (JPM, BAC, C, WFC, GS); and macroeconomic indicators (unemployment, GDP growth, CPI) via the FRED API. It ingests and archives raw files, cleans and converts them to efficient Parquet format, and loads them into DuckDB for SQL-based transformations with dbt, building a star-schema warehouse. Batch (Spark) and real-time (Kafka + Flink) processes compute key metrics—default rates by credit grade, rolling volatility, Sharpe ratios, and macro correlations—and train a baseline default-prediction model. Finally, a Streamlit dashboard visualizes KPIs, time-series analyses, and model insights.

By project end, you will have a fully automated, reproducible pipeline that mimics enterprise-grade banking analytics: from raw ingestion to actionable insights. This empowers financial organizations to monitor portfolio risk in near real time, evaluate market exposures, and support data-driven lending or investment decisions—all without incurring cloud costs.

---

## Technology Stack & Roles

- **Python**: Orchestrates data ingestion (`requests`) and transformation (`pandas`), providing scripting flexibility and rapid prototyping.
- **Apache Kafka**: Distributed message broker for ingesting real-time financial and macroeconomic streams. Used for `raw_market_ohlcv` topic for streaming market data.
- **Apache Flink**: Stream processing engine for continuous analytics on live data. Successfully configured to consume real-time market data from Kafka.
- **Apache Spark**: Distributed compute platform for scalable ETL, batch processing, and ML workloads.
- **DuckDB**: Zero-config, high-performance SQL engine acting as the local data warehouse for ad-hoc analytics.
- **Parquet**: Columnar storage format in `data/staging`—reduces file size and accelerates read performance.
- **dbt**: SQL-based transformations, testing, and documentation—converts raw tables into a clean star schema with data lineage and automated tests.
- **Streamlit**: Builds an interactive web dashboard for KPI cards and visualizations, enabling stakeholders to explore metrics and predictions in real time.
- **Airflow (DAGs)**: Orchestrates and schedules all pipeline steps via Airflow DAGs (Docker Compose), replacing any cron/Makefile workflows.
- **GitHub Actions**: CI/CD that runs dbt tests and Python unit tests on every push, ensuring code reliability.

---

## Key Features

- **Data Engineering**: Automated ingestion of lending, market, and macro data.
- **Local Storage**: DuckDB acts as a self‑contained warehouse; Parquet staging for performance.
- **Transformations**: dbt models with tests, docs, and a star schema (dimensions & facts).
- **Analytics**: SQL & Python scripts compute default rates, volatility, Sharpe ratios, and correlation matrices.
- **Modeling**: Baseline logistic regression for default prediction with performance metrics (ROC/AUC).
- **Dashboard**: Streamlit app delivering KPI cards, interactive time-series charts, and model threshold explorer.
- **Real-time & Batch Processing**: Kafka + Flink for streaming analytics; Spark for scalable ETL and batch jobs.
- **Automation**: Airflow DAGs (Docker Compose) schedule your end-to-end pipeline runs.
- **CI/CD**: GitHub Actions to validate code quality and data tests on every push.

---

## Architecture Diagram

```text
 ┌───────────┐     fetch & parquetize    ┌───────────┐
 │ Raw Data  │ ────────────────────────▶ │ staging/  │
 │ (CSV, API)│                           │ (Parquet) │
 └───────────┘                           └───────────┘
      │                                      │
      │                                      │
      ▼                                      ▼
 ┌───────────┐      dbt models & tests  ┌───────────┐
 │ DuckDB    │ ◀────────────────────────│ dbt/      │
 │ warehouse │                          │ models/   │
 └───────────┘                          └───────────┘
      │                                     │
      │ Python & SQL analytics              │
      ▼                                     ▼
 ┌───────────┐                           ┌───────────┐
 │ Kafka     │ ◀───────────────────────  │ Market    │
 │ (Broker)  │                           │ Data      │
 └───────────┘                           │ Producer  │
      │ Stream Ingestion                     └───────────┘
      │                                       ▲
      ▼                                       │
 ┌───────────┐      Stream Processing         │
 │ Flink     │ ────────────────────────▶  ┌───────────┐
 │ (Consumer)│                            │ Spark     │
 └───────────┘                            │ (Batch    │
      │                                   │ ETL)      │
      │                                   └───────────┘
      ▼                                       │
 ┌───────────┐      Streamlit UI       ┌────────────────┐
 │ Notebooks │ ────────────────────▶   │ streamlit_app/ │
 └───────────┘                         └────────────────┘
      ▲                                       │
      │ CI/CD (GitHub Actions)                │
      └───────────────────────────────────────┘
Repository Structure
Plaintext

project/
├─ data/
│  ├─ raw/             # Raw CSVs & JSON
│  ├─ staging/         # Parquet files
│  └─ warehouse.duckdb # DuckDB warehouse file
├─ kafka/              # Kafka producer & consumer code
├─ spark_jobs/         # PySpark batch ETL scripts
├─ flink_jobs/         # Flink streaming jobs
├─ airflow/            # Docker Compose & DAGs for Airflow
├─ dbt/                # dbt project (models, tests, docs)
├─ notebooks/          # EDA & modeling notebooks
├─ streamlit_app/      # Source code for the interactive Streamlit dashboard
├─ scripts/            # Ingestion & parquetize scripts
├─ docker-compose.yml  # Services: Kafka, Zookeeper, Spark, Flink, Airflow
├─ Makefile            # Cron-friendly command shortcuts
├─ requirements.txt    # Python dependencies
└─ README.md           # This file
Folder Descriptions
Path	Purpose
data/raw/	Store original, untouched data files downloaded daily—your “ground truth” copy.
data/staging/	Store cleaned, Parquet-converted files for performance.
kafka/	Kafka producer & consumer scripts to ingest and process real-time streams.
spark_jobs/	Batch ETL and analytics scripts leveraging Spark for distributed compute.
flink_jobs/	Stream processing jobs using Flink for continuous analytics.
airflow/	Configuration for orchestrating pipeline via Airflow (Docker Compose + DAGs).
dbt/	SQL transformation logic, tests, and documentation for building a star schema data warehouse.
notebooks/	Jupyter notebooks for exploratory data analysis and model development.
streamlit_app/	Source code for the interactive Streamlit dashboard.
scripts/	Python scripts for initial data ingestion (fetch_raw.py) and conversion (parquetize.py).
.gitignore	Lists files and folders Git ignores (e.g., data/raw/, virtual environments, compiled files).
Makefile	Defines shortcuts like make ingest, make transform, and make dashboard—enabling cron-friendly automation.
docker-compose.yml	Configuration for spinning up local Kafka, Zookeeper, Spark, Flink, and Airflow services.
requirements.txt	Python dependencies (e.g., requests, pandas, confluent-kafka, pyspark, apache-flink, duckdb, dbt, streamlit).

You'll need to grab an API key from: https://fredaccount.stlouisfed.org/