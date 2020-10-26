## Step 1 import Libraries 
import datetime

from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 

## Step 2 Define Default Args 
default_args = {
    'owner':"airflow",
    'depends_on_past':True,
    'start_date': days_ago(1),
    'retries':1,
    'retry_delay' : timedelta(minutes=5),
    'email':'rkchatti@gmail.com',
    'email_on_failure':True,
    'email_on_retry':True
}

## Initiate DAG 
dag = DAG(
    'my_real_first_dag',
    default_args = default_args,
    schedule_interval = '0 * * * *'
)

## Define Tasks 
t1 = BashOperator(
    task_id = 'bash_task',
    bash_command = 'echo Hello World bash.',
    dag = dag
)

def default_py_prog():
    print("Hello World Python")

t2 = PythonOperator(
    task_id = 'Python_Task',
    python_callable = default_py_prog, 
    dag = dag
)


## Step 5 Dependencies
t1 >> t2