import datetime
from airflow.decorators import dag, task
from datetime import datetime, timedelta



default_args = {
    'owner': 'Harsh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='cron_expression_dag_v4', default_args=default_args, start_date=datetime(2025, 1, 2), schedule_interval='0 3 * * Tue-Fri')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Harsh',
            'last_name': 'Singh'
        }
    
    @task()
    def get_age():
        return 24
    

    @task()
    def greet(first_name, last_name, age):
        return f"Hello {first_name} {last_name} from DAG, You are {age} years old!"
    

    name_dict = get_name()
    age = get_age()

    greet(
        first_name=name_dict['first_name'],
        last_name=name_dict['last_name'],
        age=age
    )


greet_dag = hello_world_etl()
