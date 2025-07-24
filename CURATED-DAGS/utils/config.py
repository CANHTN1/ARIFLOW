import os
import json
import yaml
from datetime import datetime, timedelta
from jinja2 import Template
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_config(config_name, context_vars=None):
    """
    Load configuration from a YAML file, optionally rendering it with Jinja2.

    Args:
        config_name (str): The path to the YAML configuration file.
        context_vars (dict, optional): Variables for Jinja2 template rendering.

    Returns:
        dict: Loaded configuration as a dictionary.
    """
    with open(config_name, 'r') as file:
        raw_content = file.read()

    if context_vars:
        template = Template(raw_content)
        rendered_content = template.render(**context_vars)
    else:
        rendered_content = raw_content

    config = yaml.safe_load(rendered_content)
    return config

def jdbc_connection(jdbc_conn_id):
    """
    Get JDBC connection details from Airflow connection.

    Args:
        jdbc_conn_id (str): The connection ID.

    Returns:
        dict: Connection details including URL, user, password, and driver.
    """
    conn = BaseHook.get_connection(jdbc_conn_id)
    extra_conn = conn.extra_dejson
    return {
        "url": conn.host,
        "user": conn.login,
        "password": conn.password,
        "jdbc_driver": extra_conn.get("driver_class")
    }

def get_tbl_latest_cob_dt(tbl_name: str, default_cob_dt: str) -> str:
    """
    Get the latest pulled COB_DT for a table based on Postgres Datalake log.

    Args:
        tbl_name (str): Name of the table.
        default_cob_dt (str): Default COB_DT if no log is found.

    Returns:
        str: Latest COB_DT in 'YYYY-MM-DD' format.
    """
    pg_hook = PostgresHook(postgres_conn_id="psql_datalake_log")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cob_dt = default_cob_dt
    try:
        cursor.execute(
            "SELECT cob_dt FROM datalakelogdb.etl_tbl_load_sts WHERE process_sts = 'S' AND tbl_name = %s ORDER BY cob_dt DESC",
            (tbl_name,)
        )
        fetch_result = cursor.fetchone()
        if fetch_result:
            fetched_cob_dt = fetch_result[0]
            cob_dt = (datetime.strptime(fetched_cob_dt, "%Y-%m-%d") + timedelta(days=1)).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error fetching latest COB_DT: {e}")
    finally:
        cursor.close()
        conn.close()

    return cob_dt

def insert_process_log(conn_id: str, dag_name: str, cob_dt: str):
    """
    Insert a process log into the PostgreSQL database.

    Args:
        conn_id (str): Connection ID for PostgreSQL.
        dag_name (str): Name of the DAG.
        cob_dt (str): COB_DT for the log entry.
    """
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        INSERT INTO datalakelogdb.etl_job_log (job_name, cob_dt, run_seq, process_sts, start_dtm)
        VALUES (%s, %s, 1, 'P', NOW())
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))

def mark_success_log(conn_id: str, dag_name: str, cob_dt: str):
    """
    Mark a process as successful in the PostgreSQL database.

    Args:
        conn_id (str): Connection ID for PostgreSQL.
        dag_name (str): Name of the DAG.
        cob_dt (str): COB_DT for the log entry.
    """
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        UPDATE datalakelogdb.etl_job_log
        SET process_sts = 'S', end_dtm = NOW(), updtd_dtm = NOW()
        WHERE job_name = %s AND cob_dt = %s
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))