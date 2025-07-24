from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
import os
from landing.utils.config import load_config, jdbc_connection, insert_process_log, mark_success_log
from airflow.operators.python import PythonOperator
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "vmdung",
    'start_date': datetime(2025, 4, 15, tzinfo=local_tz),
    'concurrency': 1,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    'email': ["vmdung@vietinbank.vn", "stgadmin@vietinbank.vn"],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
        dag_id="aml_daily_staging_ingestion_dag",
        default_args=default_args,
        catchup=False,
        schedule_interval='0 7 * * *',
        tags=["DPC", "landing", "aml"],
        max_active_tasks=10,
        description="Ingest AML daily tables in groups using SparkSubmit",
) as dag:
    
    CONFIG_DIR = f"{Variable.get('LANDING_JOB_DIR')}/src/config/aml"
    cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}"
    datalakelog_conn = BaseHook.get_connection("jdbc-postgres-datalakelog")

    spark_conf = {
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
        'spark.executor.memory': '2G',
        'spark.driver.memory': '2G',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.task.maxFailures': '5',
        'spark.yarn.queue': 'default',
        'spark.yarn.appMasterEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH'),
        'spark.yarn.executorEnv.PYSPARK_PYTHON': Variable.get('PYSPARK_VENV_PATH')
    }
    
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

    yml_files = sorted([
        f for f in os.listdir(CONFIG_DIR)
        if f.endswith(".yml") or f.endswith(".yaml")
    ])

    task_groups = []
    for yml_file in yml_files:
        config_path = os.path.join(CONFIG_DIR, yml_file)
        group_config = load_config(config_path, context_vars={"cob_dt": cob_dt})

        group_id = os.path.splitext(yml_file)[0]  # Use the file name as group_id
        with TaskGroup(group_id) as tg:
            for table_name, table_conf in group_config["tables"].items():
                flag_table = table_conf["table"]

                # Check if staging is ready
                check_flag_staging = SqlSensor(
                    task_id=f'check_flag_staging_{flag_table}',
                    conn_id="jdbc-sybase-ase",
                    sql=f"SELECT 1 FROM dbo.ETL_LOAD_STS WHERE COB_DT = '{cob_dt}' AND TBL_NAME = UPPER('{flag_table}')",
                    mode='reschedule',
                    poke_interval=600,  # Check every 10 minutes
                    timeout=21600,  # 6 hours
                )

                jdbc_info = jdbc_connection(table_conf["jdbc_conn_id"])
                jdbc_ingestion = SparkSubmitOperator(
                    task_id=f"ingest_{table_name}",
                    conn_id="spark_default",
                    conf=spark_conf,
                    archives=Variable.get('HDFS_PYSPARK_VENV'),
                    files=Variable.get('HIVE_SITE_FILE'),
                    application=f"{Variable.get('LANDING_JOB_DIR')}/src/pipeline/ingestion_jdbc.py",
                    name=f"ingestion_landing_{table_name}",
                    application_args=[
                        "--url", jdbc_info["url"],
                        "--user", jdbc_info["user"],
                        "--password", jdbc_info["password"],
                        "--jdbc_query", table_conf["query"],
                        "--jdbc_driver", jdbc_info["jdbc_driver"],
                        "--metastore_table", table_conf["metastore_table"],
                        "--repartition_num", table_conf["repartition_num"],
                        "--save_mode", table_conf["save_mode"],
                        "--save_format", table_conf["save_format"],
                    ],
                    dag=dag
                )

                metastore_table = table_conf["metastore_table"]

                write_flag_task = SparkSubmitOperator(
                    task_id=f"write_flag_{metastore_table}",
                    conn_id="spark_default",
                    name=f"landing_{metastore_table}_write_log",
                    files=Variable.get('HIVE_SITE_FILE'),
                    application=f"{Variable.get('LANDING_JOB_DIR')}/src/utils/write_flag_table.py",
                    application_args=[
                        "--url", datalakelog_conn.host,
                        "--user", datalakelog_conn.login,
                        "--password", datalakelog_conn.password,
                        "--cob_dt", cob_dt,
                        "--tbl_name", metastore_table
                    ]
                )

                check_flag_staging >> jdbc_ingestion >> write_flag_task
        
        task_groups.append(tg)

    # Final flow
    start_log_task >> task_groups >> end_log_task