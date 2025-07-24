import os
import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from curated.utils.config import load_config, jdbc_connection, insert_process_log, mark_success_log

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "pd.chinh",
    "concurrency": 1,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email": ["pd.chinh@vietinbank.vn", "stgadmin@vietinbank.vn"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="common_dtm_nii_card_dag",
    default_args=default_args,
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),
    catchup=False,
    schedule_interval="0 8 * * *",
    tags=["DPC", "curated", "common_dtm"],
    max_active_tasks=5,
    max_active_runs=1,
    description="NII Card ingestions",
) as dag:

    CONFIG_DIR = f"{Variable.get('CURATED_JOB_DIR')}/src/config/common_dtm/nii_card"
    cob_dt = "{{ logical_date.in_tz(dag.timezone) | ds }}"
    datalakelog_conn = BaseHook.get_connection("jdbc-postgres-datalakelog")

    spark_conf = {
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        "spark.executor.memory": "1G",
        "spark.driver.memory": "1G",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.task.maxFailures": "5",
        "spark.yarn.queue": "default",
        "spark.files": Variable.get("HIVE_SITE_FILE"),
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": Variable.get("PYSPARK_VENV_PATH"),
        "spark.yarn.executorEnv.PYSPARK_PYTHON": Variable.get("PYSPARK_VENV_PATH"),
        "spark.archives": Variable.get("HDFS_PYSPARK_VENV"),
    }

    # Task ghi log khi bắt đầu DAG
    start_log_task = PythonOperator(
        task_id="start_log",
        python_callable=insert_process_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",  # Kết nối Postgres
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt,
        },
    )

    # Task ghi log khi kết thúc DAG
    end_log_task = PythonOperator(
        task_id="end_log",
        python_callable=mark_success_log,
        op_kwargs={
            "conn_id": "psql_datalake_log",  # Kết nối Postgres
            "dag_name": dag.dag_id,
            "cob_dt": cob_dt,
        },
    )

    check_flag_nii_card = SqlSensor(
        task_id='check_flag_ods_nii_card',
        conn_id="jdbc-sybase-ase",
        sql=f"SELECT 1 FROM dbo.ETL_PROCESS_LOG WHERE COB_DT = '{cob_dt}' AND UPPER(JOB_NAME) = 'BJ_ODS_NII_CARD_DLY'",
        mode='reschedule',
        poke_interval=10 * 60,  # check 10 phút một
        timeout=6 * 60 * 60,  # 6 tiếng
    )

    yml_files = sorted([
        f for f in os.listdir(CONFIG_DIR)
        if f.endswith(".yml") or f.endswith(".yaml")
    ])

    task_groups = []

    for yml_file in yml_files:
        config_path = os.path.join(CONFIG_DIR, yml_file)
        group_config = load_config(config_path, {"cob_dt": cob_dt})

        group_id = os.path.splitext(yml_file)[0]
        with TaskGroup(group_id) as tg:
            for table_name, table_conf in group_config["tables"].items():
                metastore_table = table_conf["metastore_table"]
                jdbc_info = jdbc_connection(table_conf["jdbc_conn_id"])

                jdbc_ingestion = SparkSubmitOperator(
                    task_id=f"ingest_{table_name}",
                    conn_id="spark_default",
                    conf=spark_conf,
                    application=f"{Variable.get('CURATED_JOB_DIR')}/src/pipeline/jdbc_to_iceberg.py",
                    name=f"iceberg_curated_{table_name}",
                    application_args=[
                        "--url",
                        jdbc_info["url"],
                        "--user",
                        jdbc_info["user"],
                        "--password",
                        jdbc_info["password"],
                        "--jdbc_query",
                        table_conf["query"],
                        "--jdbc_driver",
                        jdbc_info["jdbc_driver"],
                        *(["--num_partitions", table_conf["num_partitions"]] if "num_partitions" in table_conf else []),
                        *(["--partition_column", table_conf["partition_column"]] if "partition_column" in table_conf else []),
                        "--metastore_table",
                        metastore_table,
                        "--repartition_num",
                        table_conf["repartition_num"],
                    ],
                )

                write_flag_task = SparkSubmitOperator(
                    task_id=f"write_flag_{metastore_table}",
                    conn_id="spark_default",
                    name=f"landing_{metastore_table}_write_log",
                    files=Variable.get("HIVE_SITE_FILE"),
                    application=f"{Variable.get('CURATED_JOB_DIR')}/src/utils/write_flag_table.py",
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
                        metastore_table,
                    ],
                )

                jdbc_ingestion >> write_flag_task

        task_groups.append(tg)

    start_log_task >> check_flag_nii_card >> task_groups >> end_log_task