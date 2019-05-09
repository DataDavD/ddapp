# import required packages
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from emr_etl.emr_spinner import ClusterFun

clust = ClusterFun()

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

with DAG(dag_id='dd_test_v1',
         default_args=default_args,
         schedule_interval=timedelta(days=7)) as dag:

    t1 = PythonOperator(task_id='EMR_spin_up',
                        python_callable=clust.spinUp,
                        dag=dag)

    t2 = PythonOperator(task_id='EMR_spark_submit_1',
                        python_callable=clust.spksub_step,
                        dag=dag)

    t3 = PythonOperator(task_id='EMR_spin_down',
                        python_callable=clust.spinDown,
                        dag=dag)

t1 >> t2 >> t3
