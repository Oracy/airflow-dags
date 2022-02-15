import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from functions_utils.utils import alert_slack_channel, cleanup_xcom

from airflow_dags.eu_efsa_food.utils import (  # isort:skip
    create_tables_group_task,
    get_table_names,
    load_data_group_task,
)


log = logging.getLogger(__name__)

files_path = "/" + "/".join(Path(__file__).parts[1:-1]) + "/files"

create_tables_queries_path = "files/create_tables/"
load_data_files = f"{files_path}/parquet_files/"

create_table_names = get_table_names(files_path)["create_table"]
load_table_names = get_table_names(files_path)["load_table"]

docs = """
Create source tables and load data into them from CSV
Project link
[@here](https://github.com/Oracy/airflow_dags/tree/2a5529a0671f91572888aa418d828d53324d6341/eu_efsa_food)
"""

default_args = {
    "owner": "Oracy Martos",
    "schedule_interval": "@once",
    "start_date": datetime(2022, 2, 11, 6, 00, 00),
    "catchup": False,
    "retries": 2,
    "on_failure_callback": alert_slack_channel,
    "dagrun_timeout": timedelta(minutes=60),
}

dag = DAG(
    "create_and_load_source",
    default_args=default_args,
    tags=[
        "Big_Project",
        "source_data",
        "load_postgres",
    ],
    max_active_runs=1,
    on_success_callback=cleanup_xcom,
    doc_md=docs,
)

with dag:

    start_flow_task = DummyOperator(task_id="start_flow")

    create_table_tasks_group = create_tables_group_task(
        create_table_names,
        create_tables_queries_path,
        dag,
    )

    load_data_task = DummyOperator(task_id="load_data")

    load_data_tasks_group = load_data_group_task(load_table_names, load_data_files, dag)

    end_flow_task = DummyOperator(task_id="end_flow")

    start_flow_task >> create_table_tasks_group
    create_table_tasks_group >> load_data_task
    load_data_task >> load_data_tasks_group
    load_data_tasks_group >> end_flow_task

if __name__ == "__main__":
    dag.cli()
