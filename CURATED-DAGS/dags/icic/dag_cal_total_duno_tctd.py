import os
from datetime import timedelta, datetime
import json
import yaml
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.sql import SqlSensor
from utils.config import load_config, jdbc_connection, get_tbl_latest_cob_dt

local_tz = pendulum.local_timezone()
current_directory = os.path.dirname(os.path.realpath(__file__))


def last_cob_date(inputdate):
    return (inputdate.in_timezone("Asia/Ho_Chi_Minh") + timedelta(days=-1)).strftime('%Y-%m-%d')


default_args = {
    'owner': 'datalake',
    'start_date': datetime(2025, 6, 1, tzinfo=local_tz),
    'concurrency': 3,
    'max_active_runs': 2,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'email': ['anhltn7@vietinbank.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
}

spark_conf = {
    'spark.executor.cores': '2',
    'spark.executor.instances': '2',
    'spark.executor.memory': '2G',
    'spark.driver.memory': '2G',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.task.maxFailures': '1',
    'spark.yarn.queue': 'default',
    'spark.yarn.appMasterEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
    'spark.yarn.executorEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH')
}

datalakelog_conn = BaseHook.get_connection("jdbc-postgres-datalakelog")

with DAG('dag_icic_total_duno_tctd',
         default_args=default_args,
         schedule_interval="@once",
         catchup=False) as dag:

    cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}"
    config_file = 'icic_sum_duno_tctd.yml'

    CONFIG_DIR = f"{Variable.get('CURATED_JOB_DIR')}/src/config/icic"
    config_path = os.path.join(CONFIG_DIR, config_file)
    context_vars = {'cob_dt': cob_dt}
    config = load_config(config_path, context_vars=context_vars)

    metastore_table = config["target"]["table"]

    icic_sum_duno_tctd = SparkSubmitOperator(
        task_id="icic_sum_duno_tctd",
        conn_id="spark_default",
        name="icic_sum_duno_tctd",
        conf=spark_conf,
        archives=Variable.get('HDFS_PYSPARK_VENV'),
        files=f"{Variable.get('HIVE_SITE_FILE')},{Variable.get('CURATED_JOB_DIR')}/src/config/icic/icic_sum_duno_tctd.yml",
        py_files=f"{Variable.get('CURATED_JOB_DIR')}/src/utils.zip",
        application=f"{Variable.get('CURATED_JOB_DIR')}/src/pipeline/curated_scd1_process_job.py",
        application_args=[
            "--cob_dt", cob_dt,
            "--cfg_file", "icic_sum_duno_tctd.yml"
        ]
    )

    write_flag_task = SparkSubmitOperator(
        task_id=f"write_flag_{metastore_table}",
        conn_id="spark_default",
        name=f"curated_{metastore_table}_write_log",
        files=Variable.get('HIVE_SITE_FILE'),
        application=f"{Variable.get('CURATED_JOB_DIR')}/src/utils/write_flag_table.py",
        application_args=[
            "--url", datalakelog_conn.host,
            "--user", datalakelog_conn.login,
            "--password", datalakelog_conn.password,
            "--cob_dt", cob_dt,
            "--tbl_name", metastore_table
        ]
    )

    icic_sum_duno_tctd >> write_flag_task