import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from airflow_dags.eu_efsa_food.utils import (  # isort:skip
    create_tables_group_task,
    get_create_table_names,
    get_load_table_names,
    load_data_group_task,
)

log = logging.getLogger(__name__)

queries_path = "/" + "/".join(Path(__file__).parts[1:-1]) + "/queries"
files_path = "/" + "/".join(Path(__file__).parts[1:-1]) + "/parquet_files"

create_tables_queries_path = "queries/create_tables/"
load_data_files = "parquet_files/source/"

create_table_names = get_create_table_names(queries_path)
load_table_names = get_load_table_names(files_path)

docs = """
Create source tables and load data into them from CSV
Project link
[@here](https://github.com/Oracy/airflow_dags/tree/2a5529a0671f91572888aa418d828d53324d6341/eu_efsa_food)
"""

default_args = {
    "owner": "Oracy Martos",
    "schedule_interval": "@once",
    "start_date": datetime.now(),
    "catchup": False,
    "retries": 2,
    "on_failure_callback": "",  # EDIT THIS LINE, CREATE CALLBACK FUNCTION
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
    on_success_callback="",  # EDIT THIS LINE, CREATE CALLBACK FUNCTION
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

    load_data_tasks_group = load_data_group_task(load_table_names, files_path, dag)

    end_flow_task = DummyOperator(task_id="end_flow")

    start_flow_task >> create_table_tasks_group
    create_table_tasks_group >> load_data_task
    load_data_task >> load_data_tasks_group
    load_data_tasks_group >> end_flow_task

if __name__ == "__main__":
    dag.cli()
