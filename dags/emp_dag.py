from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow_SS',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# Define the DAG
with DAG(
    dag_id='emp_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:
    
    
    

    # run_beam_pipeline6 = BashOperator(
    #     task_id='run_beam_pipeline6',
    #     bash_command='python3 /opt/airflow/dags/etl_pipe_1.py',  # Update path accordingly
    # )

    emp_data_load = BashOperator(
        task_id='emp_data_load',
        bash_command='python3 /opt/airflow/beam_scripts/emp_data_load.py',  # Update path accordingly
    )


    list_dir = BashOperator(
        task_id='list_dir',
        bash_command='ls -l',  # Update path accordingly
    )

    # Task to check the working directory
    check_dir_task = BashOperator(
        task_id='check_working_directory',
        bash_command='pwd'
    )

    
    beam_dep = BashOperator(
        task_id='beam_dep',
        bash_command='pip install apache-beam[gcp] faker pyarrow'
    )


    extract_dump = BashOperator(
        task_id='extract_dump',
        bash_command='python3 /opt/airflow/beam_scripts/extract.py'
    )

    beam_dep >> extract_dump >> emp_data_load

    
