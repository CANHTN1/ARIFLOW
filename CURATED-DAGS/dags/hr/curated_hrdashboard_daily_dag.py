from calendar import monthrange
from datetime import datetime, timedelta
import os
import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from curated.utils.config import insert_process_log, mark_success_log

# Use local timezone (GMT+7)
local_tz = pendulum.local_timezone()
process_dag_name = 'hrdashboard_doris'

cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}"

default_args = {
    'owner': 'hung.ds',
    'start_date': datetime(2025, 5, 14, tzinfo=local_tz),
    'concurrency': 1,
    'max_active_runs': 2,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'email': ['hung.ds@vietinbank.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
}

spark_conf = {
    'spark.executor.cores': '4',
    'spark.executor.instances': '2',
    'spark.executor.memory': '4G',
    'spark.driver.memory': '2G',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.task.maxFailures': '5',
    'spark.yarn.queue': 'default',
    'spark.archives': Variable.get("HDFS_PYSPARK_VENV"),
    'spark.yarn.appMasterEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
    'spark.yarn.executorEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
    'spark.submit.pyFiles': f"{Variable.get('CURATED_JOB_DIR')}/src/utils.zip"
}

def last_Cob_Date(inputdate):
    return (inputdate.in_timezone("Asia/Ho_Chi_Minh") + timedelta(days=-1)).strftime('%Y-%m-%d')

with DAG(
    'curated_hrdashboard_daily_dag',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=True,
    description="Process prepare for HR Dashboard in groups using SparkSubmit",
    tags=["DPC", "curated", "hr", "hrdashboard"],
) as dag:

    # Task to log the start of the DAG
    start_log_task = PythonOperator(
        task_id="start_log",
        python_callable=insert_process_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt
        }
    )

    hrdashboard_data_job = SparkSubmitOperator(
        conn_id='spark_default',
        name='hrdashboard_data_job',
        conf=spark_conf,
        files=f"{Variable.get('HIVE_SITE_FILE')},{Variable.get('CURATED_JOB_DIR')}/src/config/hr360/hrdashboard_daily_spksbmt_config.yml",
        application=f"{Variable.get('CURATED_JOB_DIR')}/src/pipeline/hr360/hrdashboard_daily_spksbmt.py",
        task_id="hrdashboard_data_job",
        application_args=[
            "--cob_dt", cob_dt,
            "--report_dt", cob_dt,
            "--check_point", '0'
        ]
    )

    trigger = TriggerDagRunOperator(
        task_id='trigger_push_doris',
        trigger_dag_id=process_dag_name,
        conf={
            "cob_dt": cob_dt,
        },
        wait_for_completion=False
    )

    # Task to log the end of the DAG
    end_log_task = PythonOperator(
        task_id="end_log",
        python_callable=mark_success_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt
        }
    )

    start_log_task >> hrdashboard_data_job >> trigger >> end_log_task