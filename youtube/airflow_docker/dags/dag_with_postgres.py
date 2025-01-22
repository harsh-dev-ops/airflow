from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'Harsh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='postgres_operator_dag_v7',
    default_args=default_args,
    start_date=datetime(2025, 1, 9),
    schedule_interval='0 0 * * *'
) as dag:
    
    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='test_postgres',
        sql="""
            create table if not exists dag_runs(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
            )
        """
    )

    delete_data = PostgresOperator(
        task_id='delete_data_from_dag_runs',
        postgres_conn_id='test_postgres',
        sql="""
            delete from dag_runs where dt = '{{ds}}' and dag_id = '{{dag.dag_id}}';
        """
    )

    insert_data = PostgresOperator(
        task_id='insert_data_into_dag_runs',
        postgres_conn_id='test_postgres',
        sql="""
            insert into dag_runs (dt, dag_id) values('{{ds}}', '{{dag.dag_id}}');
        """
    )

    create_table >> delete_data >> insert_data
