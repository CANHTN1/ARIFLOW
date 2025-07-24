from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.sql import SqlSensor
import yaml
import os
import json

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'hung.nguyentrong',
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'concurrency': 1,
    'max_active_runs': 1,
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'email': "hung.nguyentrong@vietinbank.vn",
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
    'spark.yarn.appMasterEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
    'spark.yarn.executorEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
    'spark.sql.catalog.hive': 'org.apache.iceberg.spark.SparkCatalog',
    'spark.sql.catalog.hive.type': 'hive'
}

# Global constants in DAGs
psql_log_conn_id = 'psql_datalake_log'
datalakelog_conn = BaseHook.get_connection("jdbc-postgres-datalakelog")
tbl_name_loan_act_mstr = "hive.std.loan_act_mstr"
tbl_name_loan_act_dtls = "hive.std.loan_act_dtls"
tbl_name_loan_act_trns = "hive.std.loan_act_trns"
CONFIG_DIR = f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/config/loan"

dag_name = "std_loan_act_dag"

def insert_process_log(cob_dt, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id=psql_log_conn_id)
    sql = """
        INSERT INTO datalakelogdb.etl_job_log (job_name, cob_dt, process_sts, start_dtm)
        VALUES (%s, %s, 'P', NOW())
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))

def mark_success_log(cob_dt, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id=psql_log_conn_id)
    sql = """
        UPDATE datalakelogdb.etl_job_log
        SET process_sts = 'S', end_dtm = NOW(), updtd_dtm=NOW()
        WHERE job_name = %s AND cob_dt = %s 
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))

def dag_failure_callback(context):
    cob_date = context['logical_date'].in_tz(dag.timezone)
    pg_hook = PostgresHook(postgres_conn_id=psql_log_conn_id)
    sql = """
        UPDATE datalakelogdb.etl_job_log
        SET status = 'F', updtd_dtm = NOW()
        WHERE job_name = %s AND cob_dt = %s 
    """
    pg_hook.run(sql, parameters=(dag_name, cob_date))

with DAG(
    "std_loan_act_dag",
    default_args=default_args,
    schedule_interval='10 4 * * *',  # 4h-10
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["DPC", "standardized", "loan"],
    on_failure_callback=dag_failure_callback,
    description="loan account standardized",
) as dag:

    cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}"

    wait_for_data = SqlSensor(
        task_id="wait_for_data",
        conn_id=psql_log_conn_id,
        sql=f"""
            SELECT 1 FROM etl_tbl_load_sts WHERE cob_dt = '{cob_dt}' AND tbl_name = 'profile.prf_ln' AND process_sts = 'S' LIMIT 1
        """,
        mode="reschedule",
        timeout=6 * 60 * 60,
        poke_interval=2 * 60,
    )

    std_loan_act_mstr = SparkSubmitOperator(
        task_id="std_loan_act_mstr",
        conn_id="spark_default",
        name="std_loan_act_mstr",
        conf=spark_conf,
        archives=Variable.get('HDFS_PYSPARK_VENV'),
        files=f"{Variable.get('HIVE_SITE_FILE')},{Variable.get('STANDARDIZED_JOB_DIR')}/src/config/loan/loan_act_mstr.yml",
        py_files=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/utils.zip",
        application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/pipeline/std_scd1_process_job.py",
        application_args=[
            "--cob_dt", cob_dt,
            "--cfg_file", "loan_act_mstr.yml"
        ]
    )

    write_flag_loan_act_mstr = SparkSubmitOperator(
        task_id=f"write_flag_{tbl_name_loan_act_mstr}",
        conn_id="spark_default",
        name=f"landing_{tbl_name_loan_act_mstr}_write_log",
        files=Variable.get('HIVE_SITE_FILE'),
        application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/utils/write_flag_table.py",
        application_args=[
            "--url",
            datalakelog_conn.host,
            "--user",
            datalakelog_conn.login,
            "--password",
            datalakelog_conn.password,
            "--cob_dt",
            cob_dt,
            "--tbl_name",
            tbl_name_loan_act_mstr
        ]
    )

    std_loan_act_dtls = SparkSubmitOperator(
        task_id="std_loan_act_dtls",
        conn_id="spark_default",
        name="std_loan_act_dtls",
        conf=spark_conf,
        archives=Variable.get('HDFS_PYSPARK_VENV'),
        files=f"{Variable.get('HIVE_SITE_FILE')},{Variable.get('STANDARDIZED_JOB_DIR')}/src/config/loan/loan_act_dtls.yml",
        py_files=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/utils.zip",
        application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/pipeline/std_scd2_process_job.py",
        application_args=[
            "--cob_dt", cob_dt,
            "--cfg_file", "loan_act_dtls.yml"
        ]
    )

    write_flag_loan_act_dtls = SparkSubmitOperator(
        task_id=f"write_flag_{tbl_name_loan_act_dtls}",
        conn_id="spark_default",
        name=f"landing_{tbl_name_loan_act_dtls}_write_log",
        files=Variable.get('HIVE_SITE_FILE'),
        application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/utils/write_flag_table.py",
        application_args=[
            "--url",
            datalakelog_conn.host,
            "--user",
            datalakelog_conn.login,
            "--password",
            datalakelog_conn.password,
            "--cob_dt",
            cob_dt,
            "--tbl_name",
            tbl_name_loan_act_dtls
        ]
    )

    std_loan_act_trns = SparkSubmitOperator(
        task_id="std_loan_act_trns",
        conn_id="spark_default",
        name="std_loan_act_trns",
        conf=spark_conf,
        archives=Variable.get('HDFS_PYSPARK_VENV'),
        files=f"{Variable.get('HIVE_SITE_FILE')},{Variable.get('STANDARDIZED_JOB_DIR')}/src/config/loan/loan_act_trns.yml",
        py_files=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/utils.zip",
        application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/pipeline/std_trns_generic_job.py",
        application_args=[
            "--cob_dt", cob_dt,
            "--cfg_file", "loan_act_trns.yml"
        ]
    )

    write_flag_loan_act_trns = SparkSubmitOperator(
        task_id=f"write_flag_{tbl_name_loan_act_trns}",
        conn_id="spark_default",
        name=f"landing_{tbl_name_loan_act_trns}_write_log",
        files=Variable.get('HIVE_SITE_FILE'),
        application=f"{Variable.get('STANDARDIZED_JOB_DIR')}/src/utils/write_flag_table.py",
        application_args=[
            "--url",
            datalakelog_conn.host,
            "--user",
            datalakelog_conn.login,
            "--password",
            datalakelog_conn.password,
            "--cob_dt",
            cob_dt,
            "--tbl_name",
            tbl_name_loan_act_trns
        ]
    )

    insert_process_task = PythonOperator(
        task_id='insert_process_log',
        python_callable=insert_process_log,
        op_kwargs={"cob_dt": cob_dt}
    )

    mark_success_task = PythonOperator(
        task_id='mark_success_log',
        python_callable=mark_success_log,
        op_kwargs={"cob_dt": cob_dt}
    )

    wait_for_data >> insert_process_task >> std_loan_act_mstr >> write_flag_loan_act_mstr >> mark_success_task
    insert_process_task >> std_loan_act_dtls >> write_flag_loan_act_dtls >> mark_success_task
    insert_process_task >> std_loan_act_trns >> write_flag_loan_act_trns >> mark_success_task