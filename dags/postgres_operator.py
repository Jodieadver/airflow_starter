from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id="postgres_operator_dag_v2",
    start_date=datetime(2025, 9, 30),
    schedule='0 0 * * * ',
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id='postgres_localhost',
        sql ="""
        CREATE SCHEMA IF NOT EXISTS test;
        """
    )
    task2 = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id='postgres_localhost',
        sql ="""
        CREATE TABLE IF NOT EXISTS test.dag_runs(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
        );
        """
    )

    task3 = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id='postgres_localhost',
        sql ="""
        INSERT INTO test.dag_runs (dt, dag_id)
        VALUES (CURRENT_DATE, '{{ dag.dag_id }}')
        ON CONFLICT (dt, dag_id) DO NOTHING;
        """
    )
    task1 >> task2 >> task3