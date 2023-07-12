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
dag = DAG(dag_id='eom_refresh',
    start_date= datetime.datetime(2022, 8, 9), 
    schedule_interval= '0 18 * * *', # Once a day at 4 AM
    default_args=args,
    tags=['setl'],
    catchup=False)

# dag tasks definition
with dag:    
    # Extract task
    date_par = {'day-00': 0, 'day-03': 3, 'day-10': 10, 'day-30':30}
    eom_fetch = [
    PythonOperator(
    task_id=f'eom_{key}_fetch',
    python_callable=eom_etl.fetch,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
        }
    ) for key, val in date_par.items()
    ]

    # Transform task
    eom_xform = [
    PythonOperator(
    task_id=f'eom_{key}_xform',
    python_callable=eom_etl.data_xform,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
        }
    )for key, val in date_par.items()
    ]

    # Integrity check 1
    eom_check_column_names =[
    PythonOperator(
    task_id=f'eom_{key}_check_column_names',
    python_callable=eom_etl.check_column_names,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
        }
    )for key, val in date_par.items()
    ]

    # Integrity check 2
    eom_check_data_exist = [
    PythonOperator(
    task_id=f'eom_{key}_check_data_exist',
    python_callable=eom_etl.check_data_exist,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
        }
    )for key, val in date_par.items()
    ]

    # Load Task
    eom_load =[
    PythonOperator(
    task_id=f'eom_{key}_load',
    python_callable=eom_etl.data_load,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
         }
    )for key, val in date_par.items()
    ]
    
    # Split Transformation Exception Task
    eom_data_split_exception =[
    PythonOperator(
    task_id=f'eom_{key}_data_split_exception',
    python_callable=eom_etl.data_split_exception,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_split_var':'{{ var.value.eom_split_var }}'
        }
    )for key, val in date_par.items()
    ]

    # Key Value Split Task
    eom_split = [
    PythonOperator(
    task_id=f'eom_{key}_split',
    python_callable=eom_etl.data_split,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_split_var':'{{ var.value.eom_split_var }}'
        }
    )for key, val in date_par.items()
    ]

    # Join CID Task
    eom_cid_combine = [
    PythonOperator(
    task_id=f'eom_cid_{key}_combine',
    python_callable=eom_etl.data_cid_combine,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
    }) for key, val in date_par.items()
    ]

    #Split table load
    eom_split_load = [
    PythonOperator(
    task_id=f'eom_{key}_split_load',
    python_callable=eom_etl.append_bq_split_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_split_var':'{{ var.value.eom_split_var }}'
        }
    )for key, val in date_par.items()
    ]

    #Data transformation Task
    eom_data_transformation = [
    PythonOperator(
    task_id=f'eom_{key}_data_transformation',
    python_callable=eom_etl.data_transformation,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}',
        'eom_transformation_var':'{{var.value.eom_transformation_var}}',
        'synonym_transformation_var':'{{var.value.synonym_transformation_var}}',
        'complex_synonym_transformation_var' : '{{var.value.complex_synonym_transformation_var}}'
        }
    )for key, val in date_par.items()
    ]

    #Processed table load
    eom_processed_load = [
    PythonOperator(
    task_id=f'eom_{key}_processed_load',
    python_callable=eom_etl.append_bq_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'eom_var': '{{ var.value.eom_var }}',
        'gcp_var': '{{ var.value.gcp_var }}'
         }
    )for key, val in date_par.items()
    ]

    
    # Tasks relationship
    t_0th =eom_fetch [0]>> eom_cid_fetch [0]>> eom_xform [0]>> eom_check_column_names [0]>> eom_check_data_exist [0]>> eom_load[0]>>eom_data_split_exception[0]>> eom_cid_combine[0]>> eom_split[0]>>eom_data_transformation[0]>>eom_processed_load[0]
    t_0th =eom_split [0]>> eom_split_load[0]    
