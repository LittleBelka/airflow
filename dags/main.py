from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataHandlingOperator, DataClassificationOperator


dag = DAG('classification',
          schedule_interval='@once',
          start_date=datetime(2018, 5, 1), catchup=False)

dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

data_handling_task = DataHandlingOperator(task_id='data_handling_task', dag=dag)

data_classification_task = DataClassificationOperator(task_id='data_classification_task', dag=dag)

dummy_task >> data_handling_task
data_handling_task >> data_classification_task