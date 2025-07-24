from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    "owner": "vuongtv",
    "concurrency": 1,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email": ["vuongtv@vietinbank.vn", "stgadmin@vietinbank.vn"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# Fetch connection and parameters
datalakelog_conn = BaseHook.get_connection("jdbc-postgres-datalakelog")
cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}" 

# Spark configuration
spark_conf = {
    "spark.executor.cores": "2",
    "spark.executor.instances": "2",
    "spark.executor.memory": "2G",
    "spark.driver.memory": "2G",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.task.maxFailures": "5",
    "spark.yarn.queue": "default",
    "spark.yarn.appMasterEnv.PYSPARK_PYTHON": Variable.get("PYSPARK_VENV_PATH"),
    "spark.yarn.executorEnv.PYSPARK_PYTHON": Variable.get("PYSPARK_VENV_PATH"),
}

with DAG(
    dag_id="qlrrhd_cur_twr",
    default_args=default_args,
    start_date=datetime(2025, 5, 15),
    catchup=False,
    schedule_interval=None,  # Scheduled to run daily at 01:00
    tags=["DPC", "curated", "qlrrhd"],
    max_active_tasks=10,
    description="Ingest iconnect tables to qlcl full table using SparkSubmit",
) as dag:

    # Spark job task
    customer_journey = SparkSubmitOperator(
        task_id="calculate_qlrrhd_twr",
        conn_id="spark_default",
        archives=Variable.get("HDFS_PYSPARK_VENV"),
        files=Variable.get("HIVE_SITE_FILE"),
        application=f"{Variable.get('CURATED_JOB_DIR')}/src/pipeline/qlrr/qlrrhd_twr.py",
        name="curated_qlrrhd_twr",
        application_args=[
            "--cob_dt",
            cob_dt,
        ],
        conf=spark_conf,
    )