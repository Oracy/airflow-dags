from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine

import os
import pandas as pd


def create_tables_group_task(create_table_names, create_tables_queries, dag):
    with TaskGroup(group_id="create_table_group") as create_table_tasks_group:
        create_table_tasks = []
        for create_table in create_table_names:
            last_insert_task = PostgresOperator(
                task_id="create_table_" + create_table,
                postgres_conn_id="postgres_data_source",
                sql=f"{create_tables_queries}{create_table}.sql",
                autocommit=True,
                dag=dag,
            )
            create_table_tasks.append(last_insert_task)
    return create_table_tasks_group


def load_data_group_task(load_table_names, files_path, dag):
    with TaskGroup(group_id="load_data_group") as load_data_tasks_group:
        load_data_tasks = []
        for table_name in load_table_names:
            load_data_task = PythonOperator(
                task_id="load_data_" + table_name,
                provide_context=True,
                python_callable=read_parquet,
                op_kwargs={
                    "table_name": table_name,
                    "relative_path": files_path + f"/source/{table_name}.parquet",
                },
                dag=dag,
            )
            load_data_tasks.append(load_data_task)
    return load_data_tasks_group


def write_parquet_to_postgres(df, table_name):
    engine = create_engine("postgresql://postgres:postgres@172.20.0.2:5432/postgres")
    df.to_sql(table_name, engine, if_exists="replace")
    return f"{table_name} Writed"


def read_parquet(**kwargs):
    relative_path = kwargs.get("relative_path")
    table_name = kwargs.get("table_name")
    home_path = os.environ["HOME"]
    file_path = os.path.join(home_path, "dags", relative_path)
    df = pd.read_parquet(file_path)
    write_parquet_to_postgres(df, table_name)
    return f"{table_name} Done!"


def get_load_table_names(queries_path):
    create_table_names = []
    for root, directories, files in os.walk(queries_path):
        if "create_tables" in root:
            for name in files:
                create_table_names.append(name.replace(".sql", ""))
    return create_table_names


def get_create_table_names(files_path):
    load_table_names = []
    for root, directories, files in os.walk(files_path):
        if "source" in root:
            for name in files:
                load_table_names.append(name.replace(".parquet", ""))
    return load_table_names
