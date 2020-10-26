"""
Step 1: Import Libraries 
Step 2: Define Default Arguments 
Step 3: Instantiate a DAG 
Step 4: Define Tasks 
Step 5: Set up Dependencies between Tasks 
"""

## Step 1: Import Libraries
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.utils.dates import days_ago 

from datetime import timedelta

## Step 2: Define Default Arguments
default_args = {
    'owner': 'airflow_ravi',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email' : 'rchatti@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False, 
    'retries' : 1, 
    'retry_delay' : timedelta(minutes=5)
}


## Step 3: Instantiate a DAG 
dag = DAG(
    'Airflow_Tutorial', ## DAG Name
    default_args = default_args, ## Default_Args 
    description = 'First DAG',
    schedule_interval = '0 0 * * *')


## Step 4: Define Tasks 
t1 = BashOperator(
    task_id = 'print_date',
    bash_command = 'date',
    dag = dag
)

## Step 5: Set up Dependencies between Tasks 