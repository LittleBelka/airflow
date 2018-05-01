from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataHandlingOperator


dag = DAG('start_data_handling',
          schedule_interval='@once',
          start_date=datetime(2018, 4, 30), catchup=False)

dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

start_handling_task = DataHandlingOperator(param='My first param', task_id='start_data_handling', dag=dag)

dummy_task >> start_handling_task