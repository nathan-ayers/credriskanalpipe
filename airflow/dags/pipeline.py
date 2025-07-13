from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1) Default args for retries, owner, etc.
default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 2) Define the DAG
with DAG(
    dag_id="credisk_pipeline_fetch_raw",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule="@daily",       # run once every day
    catchup=False,                    # don’t backfill old runs
    tags=["credit_pipeline"],
) as dag:

    # 3) Task: run your fetch_raw.py script
    t1_fetch = BashOperator(
        task_id="fetch_raw_data",
        bash_command=(
            "cd {{ dag_run.conf.get('project_root','/home/pog/projects/credriskanalpipe') }} && "
            "python3 scripts/fetch_raw.py"
        )
    )

    # 2) convert CSV → Parquet
    t2_parquet = BashOperator(
        task_id="parquetize_data",
        bash_command=(
            "cd /home/pog/projects/credriskanalpipe && "
            "python3 scripts/parquetize.py"
        ),
    )

    t1_fetch >> t2_parquet
