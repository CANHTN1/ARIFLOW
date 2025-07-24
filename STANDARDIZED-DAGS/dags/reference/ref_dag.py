from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.sql import SqlSensor    
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from standardized.utils.config import load_config, insert_process_log, mark_success_log

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'pd.chinh',
    'start_date': datetime(2024, 1, 1, tzinfo=local_tz),
    'concurrency': 1,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'email': ["pd.chinh@vietinbank.vn"],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    "std_ref_dag",
    default_args=default_args,
    schedule="0 5 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["DPC", "standardized", "reference"],
    description="Standardized Ref"
) as dag:

    cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}" 
    CONFIG_DIR = f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/config/reference"

    # Task ghi log khi bắt đầu DAG
    start_log_task = PythonOperator(
        task_id="start_log",
        python_callable=insert_process_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt
        }
    )

    # Task ghi log khi kết thúc DAG
    end_log_task = PythonOperator(
        task_id="end_log",
        python_callable=mark_success_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt
        }
    )

    spark_conf = {
        'spark.executor.cores': '1',
        'spark.executor.instances': '1',
        'spark.executor.memory': '1G',
        'spark.driver.memory': '1G',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.task.maxFailures': '5',
        'spark.yarn.queue': 'default',
        'spark.yarn.appMasterEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
        'spark.yarn.executorEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
        'spark.archives': Variable.get("HDFS_PYSPARK_VENV"),
        'spark.submit.pyFiles': f'{Variable.get("STANDARDIZED_JOB_DIR")}/src/utils.zip',
    }

    def generate_sql_flag_check(table_name: str, cob_dt):
        """Generate SQL query to check landing flag."""
        sql = f"SELECT count(*) FROM datalakelogdb.etl_tbl_load_sts WHERE tbl_name = '{table_name}'"
        if cob_dt:
            sql += f" AND cob_dt = '{cob_dt}'"
        return sql

    yml_files = sorted([
        f for f in os.listdir(CONFIG_DIR) if f.endswith(".yml") or f.endswith(".yaml")
    ])    

    with TaskGroup("std_ref") as tg:
        for yml_file in yml_files:
            config_path = os.path.join(CONFIG_DIR, yml_file)
            table_conf = load_config(config_path)

            source_table = table_conf["source"]["table"]
            target_table = table_conf["target"]["table"]

            check_landing_flag_task = SqlSensor(
                task_id=f"check_flag_{source_table}",
                conn_id="jdbc-postgres-datalakelog",
                sql=generate_sql_flag_check(source_table, cob_dt),
                mode='reschedule',
                poke_interval=5 * 60,
                timeout=6 * 3600,         
            )

            std_load_task = SparkSubmitOperator(
                task_id=f"process_{target_table}",
                conn_id="spark_default",
                name=f"process_{target_table}",
                conf=spark_conf,
                files=f"{Variable.get('HIVE_SITE_FILE')},{Variable.get('STANDARDIZED_JOB_DIR')}/src/config/reference/{yml_file}",
                application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/pipeline/std_scd1_process_job.py",
                application_args=[
                    "--cob_dt",
                    cob_dt,
                    "--cfg_file",
                    yml_file
                ]
            )

            check_landing_flag_task >> std_load_task    

    start_log_task >> tg >> end_log_task