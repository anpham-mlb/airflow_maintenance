import datetime
from datetime import timedelta
import dcm_etl
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import sdmp_data_push_online

# dag parameters setting
args = {
    'owner': 'Samsung',
    'is_paused_upon_creation': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=20)
}

# dag initialization
dag = DAG(dag_id='campaign_manager',
    start_date = datetime.datetime(2022, 8, 9), # this is UTC timezone in gcp cloud composer
    schedule_interval= '0 19,4 * * *', #5AM and 2PM AEST
    default_args=args,
    tags=['samsung_etl'],
    catchup=False)

# dag tasks definition
with dag:    
    # Extract task
    campaign_manager_fetch = PythonOperator(
    task_id='campaign_manager_fetch',
    python_callable=dcm_etl.fetch,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })  

    # Extract task for cid
    campaign_manager_cid_fetch = PythonOperator(
    task_id='campaign_manager_cid_fetch',
    python_callable=dcm_etl.cid_fetch,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })    

    # Transform task
    campaign_manager_xform = PythonOperator(
    task_id='campaign_manager_xform',
    python_callable=dcm_etl.data_xform,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })

    # Integrity check 1
    campaign_manager_check_column_names = PythonOperator(
    task_id='campaign_manager_check_column_names',
    python_callable=dcm_etl.check_column_names,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })

    # Integrity check 2
    campaign_manager_check_data_exist = PythonOperator(
    task_id='campaign_manager_check_data_exist',
    python_callable=dcm_etl.check_data_exist,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })

    # Load Task
    campaign_manager_load = PythonOperator(
    task_id='campaign_manager_load',
    python_callable=dcm_etl.data_load,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })
    
    # Split Transformation Exception Task
    campaign_manager_data_split_exception = PythonOperator(
    task_id='campaign_manager_data_split_exception',
    python_callable=dcm_etl.data_split_exception,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_split_config':'{{ var.value.cm_split_config }}'
    })

    # Key Value Split Task
    campaign_manager_split = PythonOperator(
    task_id='campaign_manager_split',
    python_callable=dcm_etl.data_split,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_split_config':'{{ var.value.cm_split_config }}'
    })

    #Join CID Task
    campaign_manager_cid_combine = PythonOperator(
    task_id='campaign_manager_cid_combine',
    python_callable=dcm_etl.data_cid_combine,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })

    #Split table load
    campaign_manager_split_load = PythonOperator(
    task_id='campaign_manager_split_load',
    python_callable=dcm_etl.append_bq_split_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_split_config':'{{ var.value.cm_split_config }}'
    })

    #Data transformation Task
    campaign_manager_data_transformation = PythonOperator(
    task_id='campaign_manager_data_transformation',
    python_callable=dcm_etl.data_transformation,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_transformation_config':'{{var.value.cm_transformation_config}}',
        'synonym_transformation_config':'{{var.value.synonym_transformation_config}}',
        'complex_synonym_transformation_config' : '{{var.value.complex_synonym_transformation_config}}'
    })

    #Processed table load
    campaign_manager_processed_load = PythonOperator(
    task_id='campaign_manager_processed_load',
    python_callable=dcm_etl.append_bq_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })

    #Processed table combined
    campaign_manager_data_combined = PythonOperator(
    task_id='campaign_manager_data_combined',
    python_callable=dcm_etl.data_combined,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{ var.value.cm_config}}',
        'gcp_config': '{{ var.value.gcp_config }}'
    })

    #Processed table mini combined
    campaign_manager_data_combined_mini = PythonOperator(
    task_id='campaign_manager_data_combined_mini',
    python_callable=dcm_etl.data_combined_mini,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': 0,
        'cm_config': '{{var.value.cm_config}}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_transformation_mini_config':'{{var.value.cm_transformation_mini_config}}',
        'synonym_transformation_config' : '{{var.value.synonym_transformation_config}}',
        'complex_synonym_transformation_config' : '{{var.value.complex_synonym_transformation_config}}'
    })

    # Push data to sdmp bucket
    campaign_manager_sdmp_data_push = PythonOperator(
    task_id = 'campaign_manager_sdmp_data_push',
    python_callable = sdmp_data_push_online.sdmp_data_push_online,
    op_kwargs={
        'exec_date':'{{ ds }}',
        'lapse_days':0,
        'api_config': '{{var.value.cm_config}}',
        'gcp_config':'{{ var.value.gcp_config }}', 
        'sdmp_data_push_online_config':'{{ var.value.sdmp_data_push_online_config}}',
        'synonym_transformation_config' : '{{var.value.synonym_transformation_config}}',
        'complex_synonym_transformation_config' : '{{var.value.complex_synonym_transformation_config}}'
        }
    )

    # Tasks relationship
    campaign_manager_fetch >> campaign_manager_cid_fetch >> campaign_manager_xform >> campaign_manager_check_column_names >> campaign_manager_check_data_exist >> campaign_manager_load >> campaign_manager_data_split_exception >> campaign_manager_cid_combine >> campaign_manager_split >> campaign_manager_data_transformation >> campaign_manager_processed_load >> campaign_manager_data_combined >> campaign_manager_data_combined_mini
    campaign_manager_split >> campaign_manager_split_load
    campaign_manager_data_transformation >> campaign_manager_sdmp_data_push