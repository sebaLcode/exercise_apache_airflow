from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

TAGS = ['PythonDataFlow']
DAG_ID = "mi_primer_DAG"
DAG_DESCRIPTION = "Mi primer DAG con Airflow"
DAG_SCHEDULE = "0 9 * * *"
default_args = {
    "start_date": datetime(2024, 6, 1),
    
}
retries = 4
retries_delay = timedelta(minutes=5)


def execute_task():
    print("¡Hola, Airflow! Esta es mi primera tarea en un DAG.")
    

dag = DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    catchup=False,
    schedule=DAG_SCHEDULE,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    tags=TAGS
)

with dag as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")
    
    first_task = PythonOperator(
        task_id="python_task",
        python_callable=execute_task,
        retries=retries,
        retry_delay=retries_delay
    )
    

start_task >> first_task >> end_task
