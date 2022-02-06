from airflow.decorators import task
from airflow_dags.example.utils import gen_cicle, performance
from datetime import datetime, timedelta

@task(task_id="calculate_time")
@performance("calculate_time")
def calculate_time():
    to_wakeup = datetime(2022, 2, 6, 6, 30, 00)
    cicle_to_wakeup = timedelta(minutes=90)
    for gen in gen_cicle(to_wakeup, cicle_to_wakeup):
        print(gen)

