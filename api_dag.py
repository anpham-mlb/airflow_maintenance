import datetime
from datetime import timedelta
import eom_etl
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

# dag parameters setting
args = {
    'owner': 'self',
    'is_paused_upon_creation': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=20)
}

# dag initialization
dag = DAG(dag_id='self',
    start_date = datetime.datetime(2022, 8, 9),
    schedule_interval= '0 19,4 * * *', #5AM and 2PM AEST
    default_args=args,
    tags=['samsung_etl'],
    catchup=False)

# dag tasks definition
with dag:    
    # Extract task
    self_fetch = PythonOperator(
    task_id='self_fetch',
    python_callable=eom_etl.fetch,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    })  

    # Transform task
    self_xform = PythonOperator(
    task_id='self_xform',
    python_callable=eom_etl.data_xform,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    })

    # Integrity check 1
    self_check_column_names = PythonOperator(
    task_id='self_check_column_names',
    python_callable=eom_etl.check_column_names,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    })

    # Integrity check 2
    self_check_data_exist = PythonOperator(
    task_id='self_check_data_exist',
    python_callable=eom_etl.check_data_exist,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    })

    # Load Task
    self_load = PythonOperator(
    task_id='self_load',
    python_callable=eom_etl.data_load,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    })
    
    # Split Transformation Exception Task
    self_data_split_exception = PythonOperator(
    task_id='self_data_split_exception',
    python_callable=eom_etl.data_split_exception,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_split_var':'{{ var.value.eom_split_var }}'
    })

    # Key Value Split Task
    self_split = PythonOperator(
    task_id='self_split',
    python_callable=eom_etl.data_split,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_split_var':'{{ var.value.eom_split_var }}'
    })

    #Join CID Task
    self_cid_combine = PythonOperator(
    task_id='self_cid_combine',
    python_callable=eom_etl.data_cid_combine,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    })

    #Split table load
    self_split_load = PythonOperator(
    task_id='self_split_load',
    python_callable=eom_etl.append_bq_split_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_split_var':'{{ var.value.eom_split_var }}'
    })
    # Tasks relationship
    self_fetch >> self_cid_fetch >> self_xform >> self_check_column_names >> self_check_data_exist >> self_load >> self_data_split_exception >> self_cid_combine >> self_split >> self_data_transformation
    self_split >> self_split_load
