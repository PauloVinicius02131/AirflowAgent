# dag para printar qualquer coisa na tela e no log de teste

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_something():
    print("Hello, Airflow!")
    return "Printed something in the logs." 

with DAG(
    'dag_smoke_celery_logs',
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:
    
    task_print = PythonOperator(
        task_id='print_something',
        python_callable=print_something
    )
    task_print
    
