import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from airflow_dags.example.cicle_to_sleep.utils import calculate_time

log = logging.getLogger(__name__)

docs = """
# Here you can find which time to sleep
"""

default_args = {
    "owner": "Oracy Martos",
    "schedule_interval": "*/30 22 * * *",
    "start_date": datetime(2021, 1, 1),
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": "",  # EDIT THIS LINE, CREATE CALLBACK FUNCTION
    "dagrun_timeout": timedelta(minutes=60),
}

dag = DAG(
    "cicle_to_sleep",
    default_args=default_args,
    tags=["sleep_cicle", "sleep_better"],
    max_active_runs=1,
    on_success_callback="",  # EDIT THIS LINE, CREATE CALLBACK FUNCTION
    doc_md=docs,
)

with dag:

    start_flow = DummyOperator(
        task_id="start_flow",
    )

    calculate_time = calculate_time()

    end_flow = DummyOperator(
        task_id="end_flow",
    )

    start_flow >> calculate_time
    calculate_time >> end_flow


if __name__ == "__main__":
    dag.cli()
