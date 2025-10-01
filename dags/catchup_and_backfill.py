from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="dag_with_catchup_v4",
    description="My first DAG",
    default_args=default_args,
    start_date = datetime(2025, 9, 20),
    schedule='@daily',
    catchup = True
) as dag:
    task1 = BashOperator(
        task_id="first_task",
        bash_command="echo 'My first DAG is here'"
    )

    task1

