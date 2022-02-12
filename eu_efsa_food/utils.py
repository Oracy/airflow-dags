import os
from typing import List

import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine


def get_create_table_names(queries_path) -> List:
    """Get all table names that would be created, from create queries on ./queries/create_tables/*.sql.

    Args:
        queries_path: File path that queries are stored, to read it on local folders.

    Returns:
        An array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    create_table_names = []
    for root, directories, files in os.walk(queries_path):
        if "create_tables" in root:
            for name in files:
                create_table_names.append(name.replace(".sql", ""))
    return create_table_names


def get_load_table_names(files_path) -> List:
    """Get all file names that would be loaded, from parquet files on
    ./parquet_files/source/*.parquet.

    Args:
        files_path: File path that parquet are stored, to read it on local folders.

    Returns:
        An array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
    load_table_names = []
    for root, directories, files in os.walk(files_path):
        if "source" in root:
            for name in files:
                load_table_names.append(name.replace(".parquet", ""))
    return load_table_names


def create_tables_group_task(create_table_names, create_tables_queries_path, dag) -> TaskGroup:
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
        create_table_tasks = []
        for create_table in create_table_names:
            last_insert_task = PostgresOperator(
                task_id="create_table_" + create_table,
                postgres_conn_id="postgres_data_source",
                sql=f"{create_tables_queries_path}{create_table}.sql",
                autocommit=True,
                dag=dag,
            )
            create_table_tasks.append(last_insert_task)
    return create_table_tasks_group


def load_data_group_task(load_table_names, files_path, dag) -> TaskGroup:
    """Create group task on flow with parquet files that will be loaded on database.

    Args:
        load_table_names: An array with table name that whould be loaded.
        files_path: Path where parquet files are stored.
        dag: dag parameter to set operator.

    Returns:
        A TaskGroup Operator with an array with all create tables. For example:
        ["acute_g_day_bw_all_days", "acute_g_day_bw_cons_days", "chronic_g_day_bw_to_t_pop", ...]
    """
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


def read_parquet(**kwargs) -> None:
    """Read data from parquet to call function to insert data into postgres database.

    Args:
        **relative_path: File path that parquet are stored, to read it on local folders.
        **table_name: Which database that dataframe would be inserted.

    Returns:
        Only a f-string with which table were inserted. For example:
        acute_g_day_bw_all_days Done!
    """
    relative_path = kwargs.get("relative_path")
    table_name = kwargs.get("table_name")
    home_path = os.environ["HOME"]
    file_path = os.path.join(home_path, "dags", relative_path)
    df = pd.read_parquet(file_path)
    write_parquet_to_postgres(df, table_name)


def write_parquet_to_postgres(df, table_name) -> None:
    """Insert data from parquet to postgres database using sqlalchemy create_engine.

    Args:
        df: Whole dataframe that will be insert on database, use this function only with small DFs.
        table_name: Which database that dataframe would be inserted.

    Returns:
        Only a f-string with which table were inserted. For example:
        acute_g_day_bw_all_days Writed!
    """
    engine = create_engine("postgresql://postgres:postgres@172.20.0.2:5432/postgres")
    df.to_sql(table_name, engine, if_exists="replace")
