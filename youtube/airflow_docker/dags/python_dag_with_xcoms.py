from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime


def greet(some_dict, ti):
    print(some_dict)
    name = ti.xcom_pull(task_ids='get_name', key='name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    return f"Hello {name} from DAG, You are {age} years old!"


def get_name(ti):
    ti.xcom_push(key='name', value='Harsh')

def get_age(ti):
    ti.xcom_push(key='age', value=24)

with DAG(
    dag_id='python_dag_with_xcoms_v3',
    start_date=datetime(2025, 1, 20, 3),
    schedule='@daily',
) as dag:
    
    task = PythonOperator(
        task_id='xcom_task',
        python_callable=greet,
        op_kwargs={'some_dict':{'a': 1, 'b': 2}}
    )

    task1 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task2 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
    )

    [task1, task2] >> task
