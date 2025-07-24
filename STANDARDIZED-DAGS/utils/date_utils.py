from jinja2 import Template
import yaml
import os
import json
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_config(config_name, context_vars=None):
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
    conn = BaseHook.get_connection(jdbc_conn_id)
    extra_conn = conn.extra_dejson
    return {
        "url": conn.host,
        "user": conn.login,
        "password": conn.password,
        "jdbc_driver": extra_conn.get("driver_class")
    }

def get_tbl_latest_cob_dt(tbl_name: str, default_cob_dt: str):
    """
    Get the latest pulled COB_DT for a table based on Postgres Datalake log.
    COB_DT is of the format 'YYYY-MM-DD'.
    
    :param tbl_name: Name of the table.
    :param default_cob_dt: If no log was found, return this date.
    """
    pg_hook = PostgresHook(postgres_conn_id="psql_datalake_log")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cob_dt = default_cob_dt
    cursor.execute(
        f"SELECT cob_dt FROM datalakelogdb.etl_tbl_load_sts WHERE process_sts = 'S' AND tbl_name = '{tbl_name}' ORDER BY cob_dt DESC"
    )
    
    fetch_result = cursor.fetchone()
    if fetch_result:
        fetched_cob_dt = fetch_result[0]
        cob_dt = datetime.strptime(fetched_cob_dt, "%Y-%m-%d") + timedelta(days=1)
        cob_dt = cob_dt.strftime('%Y-%m-%d')

    return cob_dt

def insert_process_log(conn_id, dag_name, cob_dt):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        INSERT INTO datalakelogdb.etl_job_log (job_name, cob_dt, run_seq, process_sts, start_dtm)
        VALUES (%s, %s, 1, 'P', NOW())
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))

def mark_success_log(conn_id, dag_name, cob_dt):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        UPDATE datalakelogdb.etl_job_log
        SET process_sts = 'S', end_dtm = NOW(), updtd_dtm = NOW()
        WHERE job_name = %s AND cob_dt = %s
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))