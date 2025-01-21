from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

dag_default_args = {
    "owner": "Harsh",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="our_first_dag_v3",
    default_args=dag_default_args,
    description="This is our first DAG.",
    start_date=datetime.today(),
    schedule_interval="@daily"
) as dag:

    task1 = BashOperator(
        task_id="task_1",
        bash_command="echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id="task_2",
        bash_command="echo hello world, this is the second task!"
    )

    task3 = BashOperator(
        task_id="task_3",
        bash_command="echo hello world, this is the third task!"
    )

    # task1.set_downstream(task2)
    # task2.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3
    task1 >> [task2, task3]
