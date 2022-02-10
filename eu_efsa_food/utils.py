from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

def create_tables_group_task(create_table_names, create_tables_queries, dag):
    with TaskGroup(group_id='create_table_group') as create_table_tasks_group:
        create_table_tasks = []
        for create_table in create_table_names:
            last_insert_task = PostgresOperator(
                task_id=create_table,
                postgres_conn_id="postgres_data_source",
                # sql=f"{queries_path}/create_tables/{create_table}.sql",
                sql=f"{create_tables_queries}{create_table}.sql",
                autocommit=True,
                dag=dag,
            )
            create_table_tasks.append(last_insert_task)
    return create_table_tasks_group
