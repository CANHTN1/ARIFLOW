from calendar import monthrange
from datetime import datetime, timedelta
import os
import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

# Use local timezone (GMT+7)
local_tz = pendulum.local_timezone()

CONFIG_DIR = f"{Variable.get('LANDING_JOB_DIR')}/src/config/kpi"
cob_dt = "{{ dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
cob_dt_prev = "{{ (dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)) + macros.timedelta(days=-1)).strftime('%Y-%m-%d')) }}"
cob_dt_nodash = "{{ dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)).strftime('%Y-%m-%d')).replace('-', '') }}"
cob_dt_next_nodash = "{{ (dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)) + macros.timedelta(days=1)).strftime('%Y-%m-%d')).replace('-', '') }}"
cob_dt_prev_nodash = "{{ (dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)) + macros.timedelta(days=-1)).strftime('%Y-%m-%d')).replace('-', '') }}"

default_args = {
    'owner': 'hung.ds',
    'start_date': datetime(2024, 12, 3, tzinfo=local_tz),
    'concurrency': 2,
    'max_active_runs': 2,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'email': ['hung.ds@vietinbank.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    'move_parquet_to_iceberg_dags',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    description="Process prepare for HR dashboard in groups using SparkSubmit",
    tags=["DPC", "curated", "hr", "hr360"],
) as dag:

    move_parquet_to_iceberg = SparkSubmitOperator(
        conn_id='spark_datalake',
        name='move_parquet_to_iceberg',
        application=f"{Variable.get('CURATED_JOB_DIR')}/src/utils/copy_parquet_to_iceberg.py",
        task_id="move_parquet_to_iceberg",
        executor_cores=2,
        executor_memory="2G",
        # Uncomment and use these if needed
        # application_args=[
        #     "--cob_dt", cob_dt
        # ],
    )