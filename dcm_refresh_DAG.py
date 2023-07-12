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
dag = DAG(dag_id='campaign_manager_refresh',
    start_date= datetime.datetime(2022, 8, 9), # this is UTC timezone in gcp cloud composer
    schedule_interval= '0 18 * * *', # Once a day at 4 AM
    default_args=args,
    tags=['samsung_etl'],
    catchup=False)

# dag tasks definition
with dag:    
    # Extract task
    date_par = {'day-00': 0, 'day-03': 3, 'day-10': 10, 'day-30':30}
    campaign_manager_fetch = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_fetch',
    python_callable=dcm_etl.fetch,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
        }
    ) for key, val in date_par.items()
    ]

    # Extract task for cid
    campaign_manager_cid_fetch = [
    PythonOperator(
    task_id = f'campaign_manager_cid_{key}_fetch',
    python_callable=dcm_etl.cid_fetch,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    }
    ) for key, val in date_par.items()
    ]  

    # Transform task
    campaign_manager_xform = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_xform',
    python_callable=dcm_etl.data_xform,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
        }
    )for key, val in date_par.items()
    ]

    # Integrity check 1
    campaign_manager_check_column_names =[
    PythonOperator(
    task_id=f'campaign_manager_{key}_check_column_names',
    python_callable=dcm_etl.check_column_names,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
        }
    )for key, val in date_par.items()
    ]

    # Integrity check 2
    campaign_manager_check_data_exist = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_check_data_exist',
    python_callable=dcm_etl.check_data_exist,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
        }
    )for key, val in date_par.items()
    ]

    # Load Task
    campaign_manager_load =[
    PythonOperator(
    task_id=f'campaign_manager_{key}_load',
    python_callable=dcm_etl.data_load,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
         }
    )for key, val in date_par.items()
    ]
    
    # Split Transformation Exception Task
    campaign_manager_data_split_exception =[
    PythonOperator(
    task_id=f'campaign_manager_{key}_data_split_exception',
    python_callable=dcm_etl.data_split_exception,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_split_config':'{{ var.value.cm_split_config }}'
        }
    )for key, val in date_par.items()
    ]

    # Key Value Split Task
    campaign_manager_split = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_split',
    python_callable=dcm_etl.data_split,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_split_config':'{{ var.value.cm_split_config }}'
        }
    )for key, val in date_par.items()
    ]

    # Join CID Task
    campaign_manager_cid_combine = [
    PythonOperator(
    task_id=f'campaign_manager_cid_{key}_combine',
    python_callable=dcm_etl.data_cid_combine,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
    }) for key, val in date_par.items()
    ]

    #Split table load
    campaign_manager_split_load = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_split_load',
    python_callable=dcm_etl.append_bq_split_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_split_config':'{{ var.value.cm_split_config }}'
        }
    )for key, val in date_par.items()
    ]

    #Data transformation Task
    campaign_manager_data_transformation = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_data_transformation',
    python_callable=dcm_etl.data_transformation,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_transformation_config':'{{var.value.cm_transformation_config}}',
        'synonym_transformation_config':'{{var.value.synonym_transformation_config}}',
        'complex_synonym_transformation_config' : '{{var.value.complex_synonym_transformation_config}}'
        }
    )for key, val in date_par.items()
    ]

    #Processed table load
    campaign_manager_processed_load = [
    PythonOperator(
    task_id=f'campaign_manager_{key}_processed_load',
    python_callable=dcm_etl.append_bq_data,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config }}',
        'gcp_config': '{{ var.value.gcp_config }}'
         }
    )for key, val in date_par.items()
    ]

    #Processed table combined
    campaign_manager_data_combined =[ 
    PythonOperator(
    task_id=f'campaign_manager_{key}_data_combined',
    python_callable=dcm_etl.data_combined,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{ var.value.cm_config}}',
        'gcp_config': '{{ var.value.gcp_config }}'
        }
    )for key, val in date_par.items()
    ]

    #Processed table mini combined
    campaign_manager_data_combined_mini =[ 
    PythonOperator(
    task_id=f'campaign_manager_{key}_data_combined_mini',
    python_callable=dcm_etl.data_combined_mini,
    op_kwargs={
        'exec_date': '{{ ds }}',
        'lapse_days': val,
        'cm_config': '{{var.value.cm_config}}',
        'gcp_config': '{{ var.value.gcp_config }}',
        'cm_transformation_mini_config':'{{var.value.cm_transformation_mini_config}}',
        'synonym_transformation_config' : '{{var.value.synonym_transformation_config}}',
        'complex_synonym_transformation_config' : '{{var.value.complex_synonym_transformation_config}}'
        }
    )for key, val in date_par.items()
    ]

    # Push data to sdmp bucket
    campaign_manager_sdmp_data_push = [
    PythonOperator(
    task_id = f'campaign_manager_{key}_sdmp_data_push',
    python_callable = sdmp_data_push_online.sdmp_data_push_online,
    op_kwargs={
        'exec_date':'{{ ds }}',
        'lapse_days':val,
        'api_config': '{{var.value.cm_config}}',
        'gcp_config':'{{ var.value.gcp_config }}', 
        'sdmp_data_push_online_config':'{{ var.value.sdmp_data_push_online_config}}',
        'synonym_transformation_config' : '{{var.value.synonym_transformation_config}}',
        'complex_synonym_transformation_config' : '{{var.value.complex_synonym_transformation_config}}'
        }
    )for key, val in date_par.items()
    ]

    # Tasks relationship
    t_0th =campaign_manager_fetch [0]>> campaign_manager_cid_fetch [0]>> campaign_manager_xform [0]>> campaign_manager_check_column_names [0]>> campaign_manager_check_data_exist [0]>> campaign_manager_load[0]>>campaign_manager_data_split_exception[0]>> campaign_manager_cid_combine[0]>> campaign_manager_split[0]>>campaign_manager_data_transformation[0]>>campaign_manager_processed_load[0]>> campaign_manager_data_combined[0] >> campaign_manager_data_combined_mini[0]
    t_0th =campaign_manager_split [0]>> campaign_manager_split_load[0]
    t_0th =campaign_manager_data_transformation[0] >> campaign_manager_sdmp_data_push[0]

    t_3rd =campaign_manager_fetch [1]>> campaign_manager_cid_fetch [1]>> campaign_manager_xform [1]>> campaign_manager_check_column_names [1]>> campaign_manager_check_data_exist [1]>> campaign_manager_load[1]>>campaign_manager_data_split_exception[1]>> campaign_manager_cid_combine[1]>> campaign_manager_split[1]>>campaign_manager_data_transformation[1]>>campaign_manager_processed_load[1]>> campaign_manager_data_combined[1] >> campaign_manager_data_combined_mini[1]
    t_3rd =campaign_manager_split [1]>> campaign_manager_split_load[1]
    t_3rd =campaign_manager_data_transformation[1] >> campaign_manager_sdmp_data_push[1]

    t_10th =campaign_manager_fetch [2]>> campaign_manager_cid_fetch [2]>> campaign_manager_xform [2]>> campaign_manager_check_column_names [2]>> campaign_manager_check_data_exist [2]>> campaign_manager_load[2]>>campaign_manager_data_split_exception[2]>> campaign_manager_cid_combine[2]>> campaign_manager_split[2]>>campaign_manager_data_transformation[2]>>campaign_manager_processed_load[2]>> campaign_manager_data_combined[2] >> campaign_manager_data_combined_mini[2]
    t_10th =campaign_manager_split [2]>> campaign_manager_split_load[2]
    t_10th =campaign_manager_data_transformation[2] >> campaign_manager_sdmp_data_push[2]

    t_30th =campaign_manager_fetch [3]>> campaign_manager_cid_fetch [3]>> campaign_manager_xform [3]>> campaign_manager_check_column_names [3]>> campaign_manager_check_data_exist [3]>> campaign_manager_load[3]>>campaign_manager_data_split_exception[3]>> campaign_manager_cid_combine[3]>> campaign_manager_split[3]>>campaign_manager_data_transformation[3]>>campaign_manager_processed_load[3]>> campaign_manager_data_combined[3] >> campaign_manager_data_combined_mini[3]
    t_30th =campaign_manager_split [3]>> campaign_manager_split_load[3]
    t_30th =campaign_manager_data_transformation[3] >> campaign_manager_sdmp_data_push[3]
    
    [t_0th, t_3rd, t_10th,t_30th]