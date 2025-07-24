from jinja2 import Template
import yaml
import os
import json
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
        "url" : conn.host,
        "user" : conn.login,
        "password" : conn.password,
        "jdbc_driver" : extra_conn.get("driver_class")
    }

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
        SET process_sts = 'S', end_dtm = NOW(), updtd_dtm=NOW()
        WHERE job_name = %s AND cob_dt = %s
    """
    pg_hook.run(sql, parameters=(dag_name, cob_dt))