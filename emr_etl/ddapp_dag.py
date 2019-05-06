# import required packages
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import emr_spinner
import ddpyspark_etl_script
import model_updt

default_args = {
    'owner': 'ddapp',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 1),
    'email': 'email@email.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(dag_id='dd_test_v1',
          default_args=default_args,
          schedule_interval=timedelta(days=7))

t1 = PythonOperator(
    task_id='py_1',
    python_callable=emr_spinner,
    dag=dag)

t2 = PythonOperator(
    task_id='py_2',
    python_callable=ddpyspark_etl_script,
    dag=dag)

t3 = PythonOperator(
    task_id='py_3',
    python_callable=model_updt,
    dag=dag)

t1 >> t2 >> t3
