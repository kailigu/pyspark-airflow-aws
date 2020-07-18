import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kaili',
    'start_date': datetime(2020, 7, 1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('example_dag',  
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='* * * 0 *',
          max_active_runs = 1       
        )

city_code = BashOperator(
    task_id='get_COUNTRY_and_CITY_CODE',
    bash_command='python /usr/local/airflow/dags/scripts/task1.py',
    dag=dag)

city = BashOperator(
    task_id='get_US_CITY_and_US_STATE',
    bash_command='python /usr/local/airflow/dags/scripts/task2.py',
    dag=dag)    

airport = BashOperator(
    task_id='get_AIRPORT',
    bash_command='python /usr/local/airflow/dags/scripts/task3.py',
    dag=dag)   

weather = BashOperator(
    task_id='get_WEATHER',
    bash_command='python /usr/local/airflow/dags/scripts/task4.py',
    dag=dag)   

test = SparkSubmitOperator(
    task_id="test",
    conn_id="spark_conn",
    application="/usr/local/airflow/dags/scripts/test.py",
    verbose=False,
    dag = dag
)

city_code >> city >> airport >> weather >> test