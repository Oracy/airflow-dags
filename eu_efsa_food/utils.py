import os
from typing import Any, Dict, List

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from functions_utils.utils import read_parquet
from sqlalchemy import create_engine


def get_table_names(files_path: str) -> List[List[str], List[str]]:
    """Get all file names that would be loaded, from parquet files on
    ./parquet_files/source/*.parquet.

    Args:
        files_path: File path that parquet are stored, to read it on local folders.

    Returns:
        An array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    load_table: List[str] = []
    create_table: List[str] = []
    for root, directories, files in os.walk(files_path):
        if "parquet_files" in root:
            load_table.append(files)
        elif "create_tables" in root:
            create_table.append(files)
    load_table = [table.replace(".parquet", "") for table in load_table[0]]
    create_table = [table.replace(".sql", "") for table in create_table[0]]
    return {"load_table": load_table, "create_table": create_table}


def create_tables_group_task(
    create_table_names: List[str], create_tables_queries_path: str, dag: DAG
) -> TaskGroup:
    """Create group task on flow with sql queries that create tables on database.

    Args:
        create_table_names: An array with table name that whould be created.
        create_tables_queries_path: Path where create queries are stored.
        dag: dag parameter to set operator.

    Returns:
        A TaskGroup Operator with an array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    with TaskGroup(group_id="create_table_group") as create_table_tasks_group:
        create_table_tasks: List[PostgresOperator] = []
        for create_table in create_table_names:
            last_insert_task: PostgresOperator = PostgresOperator(
                task_id="create_table_" + create_table,
                postgres_conn_id="postgres_data_source",
                sql=f"{create_tables_queries_path}{create_table}.sql",
                autocommit=True,
                dag=dag,
            )
            create_table_tasks.append(last_insert_task)
    return create_table_tasks_group


def load_data_group_task(
    load_table_names: List[str], load_data_files: str, dag: DAG
) -> "TaskGroup":
    """Create group task on flow with parquet files that will be loaded on database.

    Args:
        load_table_names: An array with table name that whould be loaded.
        load_data_files: Path where parquet files are stored.
        dag: dag parameter to set operator.

    Returns:
        A TaskGroup Operator with an array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    with TaskGroup(group_id="load_data_group") as load_data_tasks_group:
        load_data_tasks: List[PythonOperator] = []
        for table_name in load_table_names:
            load_data_task: PythonOperator = PythonOperator(
                task_id="load_data_" + table_name,
                provide_context=True,
                python_callable=write_parquet_to_postgres,
                op_kwargs={
                    "table_name": table_name,
                    "relative_path": f"{load_data_files}{table_name}.parquet",
                },
                dag=dag,
            )
            load_data_tasks.append(load_data_task)
    return load_data_tasks_group


def write_parquet_to_postgres(**kwargs: Dict[str, Any]) -> None:
    """Insert data from parquet to postgres database using sqlalchemy create_engine.

    Args:
        **table_name: Which database that dataframe would be inserted.

    Returns:
        Only a f-string with which table were inserted. For example:
        acute_g_day_bw_all_days Writed!
    """
    table_name: str = kwargs.get("table_name")
    df: pd.DataFrame = read_parquet(**kwargs)
    engine = create_engine("postgresql://postgres:postgres@172.25.0.2:5432/postgres")
    df.to_sql(table_name, engine, if_exists="replace")
