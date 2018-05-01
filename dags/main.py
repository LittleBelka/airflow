from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataHandlingOperator, DataClassificationOperator


dag = DAG('start_data_handling',
          schedule_interval='@once',
          start_date=datetime(2018, 5, 1), catchup=False)

dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

start_handling_task = DataHandlingOperator(param='param1', task_id='start_data_handling', dag=dag)

start_classification_task = DataClassificationOperator(param='param2', task_id='start_data_classification', dag=dag)

dummy_task >> start_handling_task
start_handling_task >> start_classification_task