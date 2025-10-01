
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# MAX XCOM size is 48KB
# if you want to push more data use external storage (S3, GCS, Database)

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name_task', key='first name')
    last_name = ti.xcom_pull(task_ids='get_name_task', key='last name')
    age = ti.xcom_pull(task_ids='get_age_task', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, and I am {age} years old") 

def get_name(ti):
    ti.xcom_push(key='first name', value='Airflow')
    ti.xcom_push(key='last name', value='Rocks!')

def get_age(ti):
    ti.xcom_push(key='age', value=5)



with DAG(
    default_args=default_args,
    dag_id = "python_operator_v6",
    description = "A simple python operator DAG",
    start_date = datetime(2024, 6, 1),
    schedule='@daily'
)as dag:

    task1 = PythonOperator(
        task_id = "python_task",
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id = "get_name_task",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id = "get_age_task",
        python_callable=get_age
    )

    [task2, task3] >> task1

    
