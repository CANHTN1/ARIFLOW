import os
from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from curated.utils.config import load_config

local_tz = pendulum.local_timezone()

cob_dt = "{{ dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)).strftime('%Y-%m-%d')) }}"
cob_dt_prev = "{{ dag_run.conf.get('cob_dt_prev', (macros.datetime.now() - macros.timedelta(days=2)).strftime('%Y-%m-%d')) }}"
cob_dt_next = "{{ dag_run.conf.get('cob_dt_next', (macros.datetime.now()).strftime('%Y-%m-%d')) }}"
cob_dt_nodash = "{{ dag_run.conf.get('cob_dt', (macros.datetime.now() - macros.timedelta(days=1)).strftime('%Y-%m-%d')).replace('-', '') }}"
cob_dt_next_nodash = "{{ dag_run.conf.get('cob_dt_next', (macros.datetime.now()).strftime('%Y-%m-%d')).replace('-', '') }}"
cob_dt_prev_nodash = "{{ dag_run.conf.get('cob_dt_prev', (macros.datetime.now() - macros.timedelta(days=2)).strftime('%Y-%m-%d')).replace('-', '') }}"
conn = BaseHook.get_connection("jdbc-doris-hr")

default_args = {
    'owner': 'hung.ds',
    'start_date': datetime(2025, 5, 18, tzinfo=local_tz),
    'concurrency': 2,
    'max_active_runs': 2,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'email': ['hung.ds@vietinbank.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG('test_bash',
         schedule_interval='0 8 * * *',
         default_args=default_args) as dag:

    # Load config from YAML
    CONFIG_DIR = f"{Variable.get('CURATED_JOB_DIR')}/src/config/hr360"
    yml_file = "hrdashboard_daily_load_to_doris_config.yml"
    config_path = os.path.join(CONFIG_DIR, yml_file)
    group_config = load_config(config_path, context_vars={
        "cob_dt": cob_dt,
        "cob_dt_next": cob_dt_next,
        "cob_dt_prev": cob_dt_prev,
        "cob_dt_nodash": cob_dt_nodash,
        "cob_dt_next_nodash": cob_dt_next_nodash,
        "cob_dt_prev_no_dash": cob_dt_prev_nodash
    })

    # Config push Doris tasks
    with TaskGroup(group_id="push_doris_group") as push_doris_group:
        for table_name, table_conf in group_config["tables"].items():
            # Each table
            hrd_delete_today = BashOperator(
                task_id=f"delete_today_partition_{table_name}",
                bash_command=(
                    f"mysql -h {conn.host} -P 9030 -u {conn.login} -p{conn.password} hrdashboard -e "
                    f"\"ALTER TABLE {table_conf['schema']}.{table_conf['table']} "
                    f"DROP PARTITION IF EXISTS p{cob_dt_nodash}\""
                )
            )

            hr_create_partition_today = BashOperator(
                task_id=f"create_today_partition_{table_name}",
                bash_command=(
                    f"mysql -h {conn.host} -P 9030 -u {conn.login} -p{conn.password} hrdashboard -e "
                    f"\"ALTER TABLE {table_conf['schema']}.{table_conf['table']} "
                    f"ADD PARTITION IF NOT EXISTS p{cob_dt_nodash} "
                    f"VALUES [('{cob_dt}'), ('{cob_dt_next}')]\""
                )
            )

            push_doris = BashOperator(
                task_id=f"push_doris_{table_name}",
                bash_command=(
                    f"sh {Variable.get('CURATED_JOB_DIR')}/src/pipeline/bash_script/doris/load_file_to_doris.sh "
                    f"--hdfs_dir {table_conf['hdfs_dir']} "
                    f"--schema {table_conf['schema']} "
                    f"--host {conn.host} "
                    f"--table {table_conf['table']} "
                    f"--user {conn.login} "
                    f"--password {conn.password} "
                    f"--columns {table_conf['columns']} "
                    f"--filetype {table_conf['filetype']}"
                )
            )

            hrd_delete_today >> hr_create_partition_today >> push_doris