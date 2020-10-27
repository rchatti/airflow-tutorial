
# Step 1 Import Libraries 
from datetime import datetime,timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

# Step 2: Define Default Args 
default_args = {
    'owner':'Airflow_Test',
    'start_date':datetime(2020,9,25),
    'end_date':datetime(2020,10,1),
    'retry_delay' : timedelta(minutes=5),
    'email':'airflow@airflow.com',
    'email_on_failure':False,
    'email_on_retry':False
}

# Step 3: Initiate DAG 
dag = DAG('Airflow_20201025',
    default_args=default_args,
    schedule_interval='0 0 * * *'
    )

# Step 4: Define Tasks 
t1 = BashOperator(
    task_id = 'Bash_Test',
    bash_command = 'date',
    dag = dag
)

def py_call(name):
    print("Hello {0}. Congrats on Printing this.".format(name))

t2 = PythonOperator(
    task_id = 'Py_Test',
    python_callable = py_call,
    op_kwargs={'name':'Ravi Kishore'},
    dag = dag
)

t3 = BigQueryCheckOperator(
    task_id = 'Check_Chicago_Crime_Table',
    sql = '''SELECT count(*) FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` where trip_start_timestamp = "{{yesterday_ds}}"''',
    bigquery_conn_id='my_gcp_conn',
    use_legacy_sql=False,
    dag = dag
)
# Step 5: Define Dependencies

t1 >> t2 >> t3
