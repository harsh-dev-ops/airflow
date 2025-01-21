from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def greet(name: str, age: int):
    return f"Hello {name} from DAG, You are {age} years old!"


with DAG(
    dag_id="python_operator_dag",
    description="DAG to handle python operator",
    start_date=datetime(2025, 1, 21, 3),
    schedule="@daily"
) as dag:
    python_task = PythonOperator(
        task_id="python_operator_task",
        python_callable=greet,
        op_kwargs={'name': 'Harsh', 'age': 24}
    )

    task2 = PythonOperator(
        task_id="python_operator_task2",
        python_callable=greet,
        op_args=('Prateek', 24)
    )

    [python_task, task2]
