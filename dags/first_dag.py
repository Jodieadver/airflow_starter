from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="first_dag_v3",
    description="My first DAG",
    default_args=default_args,
    start_date = datetime(2024, 6, 1),
    schedule='@daily'
) as dag:
    task1 = BashOperator(
        task_id="first_task",
        bash_command="echo 'My first DAG is here'"
    )

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo 'My second task will be run after task 1'"
    )

    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo 'My third task will be run after task 1 and the same time with task 2'"
    )
    # first way to set dependencies
    # task1 >> task2
    # task1 >> task3

    # Second way to set dependencies
    task1 >> [task2, task3]




