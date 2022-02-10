from airflow import DAG
from airflow_dags.eu_efsa_food.utils import create_tables_group_task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from pathlib import Path

import logging
import os

log = logging.getLogger(__name__)

queries_path = "/" + "/".join(Path(__file__).parts[1:-1]) + "/queries"
create_tables_queries = "queries/create_tables/"

create_table_names = []
for root, directories, files in os.walk(queries_path):
    if "create_tables" in root:
        for name in files:
            create_table_names.append(name.replace(".sql", ""))

docs = f"""
Create source tables and load data into them from CSV
"""

default_args = {
    "owner": "Oracy Martos",
    "schedule_interval": "@once",
    "start_date":  datetime(2021, 1, 1),
    "catchup": False,
    "retries": 2,
    "on_failure_callback": "", # EDIT THIS LINE, CREATE CALLBACK FUNCTION
    "dagrun_timeout": timedelta(minutes=60),
}

dag = DAG(
    "create_and_load_source",
    default_args=default_args,
    tags=["Big_Project", "source_data", "load_postgres"],
    max_active_runs=1,
    on_success_callback="", # EDIT THIS LINE, CREATE CALLBACK FUNCTION
    doc_md=docs
)

with dag:

    start_flow_task = DummyOperator(
        task_id='start_flow'
    )

    create_table_tasks_group = create_tables_group_task(
        create_table_names,
        create_tables_queries,
        dag
        )
    
    end_flow_task = DummyOperator(
        task_id='end_flow'
    )

    start_flow_task >> create_table_tasks_group
    create_table_tasks_group >> end_flow_task

if __name__ == "__main__":
    dag.cli()
