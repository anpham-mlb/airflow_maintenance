import io
import datetime
import time
from datetime import timedelta
import os
import pandas as pd
import numpy as np
import httplib2
import csv
from oauth2client.tools import argparser
import helpers
import json
from googleapiclient import discovery, http
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from google.cloud import bigquery
from key_value_split_func import Split
from request_api import count_header_footer_rows
from request_api import save_to_local
from request_api import delete_local_file

# Data fetching function by API calls
def fetch(exec_date, lapse_days, eom_var, gcp_var):    
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d')
        eom_var = json.loads(eom_var)
        gcp_var = json.loads(gcp_var)
        common_ops = helpers.Common_Ops(gcp_var["project_id"], gcp_var["raw_file_bucket"], eom_var["api"], 
                                        eom_var["environment"], gcp_var["gcp_cred_file_path"])
        secrets =  json.loads(common_ops.access_secret_version("SELF_EOM_CLIENT_SECRETS"))
        secret_file = 'secrets.json'
        save_to_local(secrets, secret_file)
        
        token = json.loads(common_ops.access_secret_version("SELF_EOM_CREDS_ACCESS_TOKEN"))
        token_file = 'token.json'
        save_to_local(token, token_file)
        
        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(secret_file, scope=eom_var["api_scopes"])
        
        # Check whether credentials exist in the credential store. Using a credential
        storage = oauthFile.Storage(token_file)        
        credentials = storage.get()
        
        args = argparser.parse_args(args=[])
        args.noauth_local_webserver = True

        # If no credentials were found, go through the authorization process and
        # persist credentials to the credential store.
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, args)

        print("Trying to authorize..")
        # Use the credentials to authorize an httplib2.Http instance.
        http_oauth = credentials.authorize(httplib2.Http())

        # Construct a service object via the discovery service.
        service = discovery.build(eom_var["deom_api_name"], eom_var["api_version"], http=http_oauth)
        start_date = datetime.datetime.strptime(exec_date, '%Y-%m-%d') 
        end_date = start_date

        delete_local_file('secrets.json')
        delete_local_file('token.json')

        report = {
            # Set the required fields "name" and "type".
            'name': 'EOM Reporting Raw',
            'type': 'STANDARD',
            # Set optional fields.
            'fileName': 'test',
            'format': 'CSV'
        }

        #Create a report criteria
        criteria = {
            'dateRange': {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d')
            }
        }
        # Add the criteria to the report resource.
        report['criteria'] = criteria

        # Update an existing report.
        DEOM_PROFILE_ID = common_ops.access_secret_version("SELF_EOM_PROFILE_ID")
        REPORT_ID = common_ops.access_secret_version("SELF_EOM_REPORT_ID")
        patched_report = service.reports().patch(profileId=DEOM_PROFILE_ID, reportId=REPORT_ID, body=report).execute()
        report_run = service.reports().run(profileId=DEOM_PROFILE_ID, reportId=REPORT_ID).execute()

        file_id = report_run['id']

        # Wait for the report file to finish processing.
        # An exponential da strategy is used to conserve request quota.
        sleep = 10
        start_time = time.time()
        while True:
            report_file = service.files().get(
                reportId=REPORT_ID, fileId=file_id).execute()
            status = report_file['status']
            if status == 'REPORT_AVAILABLE':
                print(f'File status is {status}, ready to download.')
                break
            elif status != 'PROCESSING' and status != 'QUEUED':
                print(f'File status is {status}, processing failed.')
                break
            elif time.time() - start_time > 100000:
                print('File processing deadline exceeded.')
                break

            # sleep = next_sleep_interval(sleep)
            print (f'File status is {status}, sleeping for {sleep} seconds.')
            time.sleep(sleep)

        report_file = service.files().get(reportId=REPORT_ID, fileId=file_id).execute()
        RAW_FILE = f'deom_raw{exec_date}.csv'
        delete_local_file(RAW_FILE)
        if report_file['status'] == 'REPORT_AVAILABLE':
            # Prepare a local file to download the report contents to.
            out_file = io.FileIO(RAW_FILE, mode='wb')

            # Create a get request.
            request = service.files().get_media(reportId=REPORT_ID, fileId=file_id)
        
        # this condition happens when dataframe is empty and slack_notification_if_nodata is "TRUE" so we can send slack notification
        elif df.empty and eom_var["slack_notification_if_nodata"] =="TRUE":
            common_ops.send_slack_notification("function fetch ran for datasource eom no data was returned for the requested day: ",exec_date)
            common_ops.save_to_pickle(df, f"eom/eom_raw_{exec_date}.pickle")
        
        # this condition happens when dataframe is empty and slack_notification_if_nodata is "FALSE" so we can switch off the slack notification options
        else:
            print('no data from eom on:', exec_date)
            common_ops.save_to_pickle(df, f"eom/eom_raw_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

# Check data
def check_data_exist(exec_date, lapse_days, eom_var, gcp_var):
    try:
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_{exec_date}.pickle")
        if (df.shape[0]) > 0:
            dateToCheck = df['Date'][0]
            sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_table"]}` where Date = "{dateToCheck}" LIMIT 1'
            df = common_ops.bq_client.query(sql).to_dataframe()
            if df.shape[0] > 0:
                print('delete ', dateToCheck)
                sql = f'DELETE FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_table"]}` where Date = "{dateToCheck}"'
                df = common_ops.bq_client.query(sql).to_dataframe()
        elif (df.shape[0]) < 0:
            print('no data from eom on:', exec_date)

        if (cid_df.shape[0]) > 0:
            dateToCheck = cid_df['Date'][0]
            sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_cid_table"]}` where Date = "{dateToCheck}" LIMIT 1'
            cid_df = common_ops.bq_client.query(sql).to_dataframe()
            if cid_df.shape[0] > 0:
                print('delete ', dateToCheck)
                sql = f'DELETE FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_cid_table"]}` where Date = "{dateToCheck}"'
                cid_df = common_ops.bq_client.query(sql).to_dataframe()        
        elif (cid_df.shape[0]) < 0:
            print('no data from eom cid on:', exec_date)
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

# Check API column names 
def check_column_names(exec_date, lapse_days, eom_var, gcp_var):
    try:
        api_df = common_ops.get_data_from_pickle(f"eom/eom_xform_{exec_date}.pickle")
        api_cid_df = common_ops.get_data_from_pickle(f"eom/eom_cid_xform_{exec_date}.pickle")
        if (api_df.shape[0]) == 0:
             print('no data from eom on:', exec_date)
        elif (api_df.shape[0]) > 0:
            api_cols = api_df.columns.tolist()
            sql = f'SELECT column_name FROM {gcp_var["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{eom_var["bq_table"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {eom_var["bq_table"]} table')  
        
        if (api_cid_df.shape[0]) == 0:
             print('no data from eom cid on:', exec_date)
        elif (api_cid_df.shape[0]) > 0:    
            api_cid_cols = api_cid_df.columns.tolist()
            sql = f'SELECT column_name FROM {gcp_var["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{eom_var["bq_cid_table"]}"'
            cid_df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = cid_df.column_name.tolist()

            mismatch = set(api_cid_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {eom_var["bq_cid_table"]} table')
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise
# Data loading function to be called by eom task
def data_load(exec_date, lapse_days, eom_var, gcp_var):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        eom_var = json.loads(eom_var)
        gcp_var = json.loads(gcp_var)
        common_ops = helpers.Common_Ops(gcp_var["project_id"], gcp_var["raw_file_bucket"], eom_var["api"], 
                                        eom_var["environment"], gcp_var["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_{exec_date}.pickle")
        cid_df = common_ops.get_data_from_pickle(f"eom/eom_cid_xform_{exec_date}.pickle")
        
        if df.empty:
            print('no data from eom on:', exec_date)
        elif len(df) > 0:
        # Insert data to big query
            dataset_ref = common_ops.bq_client.dataset(gcp_var["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(eom_var["bq_table"])
            table_var = bigquery.job.LoadJobVar(write_disposition='WRITE_APPEND')
            table_var.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=eom_var["location"], job_var=table_var)
            job.result() 

        if cid_df.empty:
            print('no data from eom cid on:', exec_date)
        elif len(cid_df) > 0:
        # Insert data to big query
            dataset_ref = common_ops.bq_client.dataset(gcp_var["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(eom_var["bq_cid_table"])
            table_var = bigquery.job.LoadJobVar(write_disposition='WRITE_APPEND')
            table_var.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(cid_df, table_ref, location=eom_var["location"], job_var=table_var)
            job.result()  
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

# Data split exception function to be called by eom task
#exec_date, eom_var, gcp_var are parameters passed by Airflow Python operator 
def data_split_exception(exec_date, lapse_days, eom_var, gcp_var,eom_split_var):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        eom_var = json.loads(eom_var)
        gcp_var = json.loads(gcp_var)
        eom_split_var = json.loads(eom_split_var)
        common_ops = helpers.Common_Ops(gcp_var["project_id"], gcp_var["raw_file_bucket"], eom_var["api"],
                                        eom_var["environment"], gcp_var["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_{exec_date}.pickle")
        if df.empty:
            print('no data from eom on:', exec_date)
            common_ops.save_to_pickle(df, f"eom/eom_xform_exception_{exec_date}.pickle")
        else:

 
            # look up campaigns to exclude
            exclude_campaigns_list = eom_split_var["exclude_campaigns_list"]
            df = df.drop(df[df['Campaign_ID'].isin(exclude_campaigns_list)].index, axis=0,inplace=False)
            
            # join Placement_cost table
            sql_placement_costs=f'SELECT * FROM seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_table_placement_costs"]}'
            df_placement_costs = common_ops.bq_client.query(sql_placement_costs).to_dataframe()
            df['Date'] =df['Date'].astype('str')
            df['Placement_ID'] =df['Placement_ID'].astype('str')
            df_placement_costs['dates']=df_placement_costs['dates'].astype('str')
            df_placement_costs['placement_ids']=df_placement_costs['placement_ids'].astype('str')
            df = df.merge(df_placement_costs, how='left', left_on=['Date','Placement_ID'],right_on= ['dates','placement_ids'])
       
            # Insert data to pickle
            common_ops.save_to_pickle(df, f"eom/eom_xform_exception_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

# Data joining function to be called by eom task. Read the transformed data in binary file from binary file in disk
# Extract cid from eom cid reports
# and join with eom reports
def data_cid_combine(exec_date, lapse_days, eom_var, gcp_var):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        eom_var = json.loads(eom_var)
        gcp_var = json.loads(gcp_var)
        common_ops = helpers.Common_Ops(gcp_var["project_id"], gcp_var["raw_file_bucket"], eom_var["api"],
                                        eom_var["environment"], gcp_var["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_exception_{exec_date}.pickle")

        if df.empty:
            print('no data from eom on:', exec_date)
            common_ops.save_to_pickle(df, f"eom/eom_cid_xform_combine_{exec_date}.pickle")
        else:
            sql = f'SELECT * FROM {gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_cid_lookup"]}'
            cid_df = common_ops.bq_client.query(sql).to_dataframe()
            # Rename columns and remove unecessary values
            cid_df = cid_df.rename(columns = {"Site_ID__EOM360_" : "Site_ID__DEOM_"})
            cid_df = cid_df.reset_index(drop = True)
            # Get only cid ending in '_pfm' if there are multiple cids for one day
            cid_df_count = cid_df.copy()
            cid_df_count['Count'] = cid_df_count.groupby(["Creative_ID", "Advertiser_ID", "Campaign_ID", "Site_ID__DEOM_", "Placement_ID", "Ad_ID"])['CID'].transform('count')
            cid_df_count = cid_df_count[cid_df_count["Count"] > 1]
            removed_cid_index = cid_df_count.index.values.tolist()
            removed_cid_index_list = []
            for i in removed_cid_index:
                if not cid_df["CID"].iloc[i].endswith("pfm"):
                    removed_cid_index_list.append(i)
            cid_df = cid_df.drop(cid_df.index[removed_cid_index_list])

            #join cid df to df
            for col in ["Creative_ID", "Placement_ID", "Advertiser_ID", "Campaign_ID", "Site_ID__DEOM_", "Ad_ID"]:
                cid_df[col] = cid_df[col].apply(lambda i: str(i))
                df[col] = df[col].apply(lambda i: str(i))
            
            df = df.merge(cid_df, on = ["Creative_ID", "Advertiser_ID", "Campaign_ID", "Site_ID__DEOM_", "Placement_ID", "Ad_ID"], how = "left")

            common_ops.save_to_pickle(df, f"eom/eom_cid_xform_combine_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

# Data loading function to be called by eom task
def data_split(exec_date, lapse_days, eom_var, gcp_var, eom_split_var):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        eom_var = json.loads(eom_var)
        gcp_var = json.loads(gcp_var)
        eom_split_var = json.loads(eom_split_var)
        common_ops = helpers.Common_Ops(gcp_var["project_id"], gcp_var["raw_file_bucket"], eom_var["api"],
                                        eom_var["environment"], gcp_var["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"eom/eom_cid_xform_combine_{exec_date}.pickle")
        if df.empty:
            print('no data from eom on:', exec_date)
            common_ops.save_to_pickle(df, f"eom/eom_xform_split_{exec_date}.pickle")
        else:
            # Apply Key Value Split Transformation
            df = Split(df,eom_split_var)
            # Insert data to pickle
            common_ops.save_to_pickle(df, f"eom/eom_xform_split_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

# This function will be called when split data is loading into 
# exec_date, eom_var, gcp_var are parameters passed by Airflow Python operator in deom_DAG.py
def append_bq_split_data(exec_date, lapse_days, eom_var, eom_split_var, gcp_var):
    try:
        # Check if data exists
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_split_{exec_date}.pickle")
        
        if (df.shape[0]) > 0:
            dateToCheck = df['DATE'][0]
            # remove the time part from timestamp
            # dateToCheck = pd.to_datetime(dateToCheck).date()
            sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_split_table"]}` where Date = "{dateToCheck}" LIMIT 1'
            df = common_ops.bq_client.query(sql).to_dataframe()
            if df.shape[0] > 0:
                print('delete ', dateToCheck)
                sql = f'DELETE FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_split_table"]}` where Date = "{dateToCheck}"'
                df = common_ops.bq_client.query(sql).to_dataframe()

            # Check column names
            api_df = common_ops.get_data_from_pickle(f"eom/eom_xform_split_{exec_date}.pickle")  
            api_df = api_df[eom_split_var["selected_columns"]]      
            api_cols = api_df.columns.tolist()
            api_cols = [col.lower() for col in api_cols]
        
            sql = f'SELECT column_name FROM {gcp_var["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{eom_var["bq_split_table"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()
            table_columns = [col.lower() for col in table_columns]

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {eom_var["bq_split_table"]} table')

            # Insert data to big query
            df = common_ops.get_data_from_pickle(f"eom/eom_xform_split_{exec_date}.pickle")
            df = df[eom_split_var["selected_columns"]]
            dataset_ref = common_ops.bq_client.dataset(gcp_var["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(eom_var["bq_split_table"])
            table_var = bigquery.job.LoadJobVar(write_disposition='WRITE_APPEND')
            table_var.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=eom_var["location"], job_var=table_var)
            job.result()
        
        else:
            print('no data from eom on:', exec_date)

    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

#Implement data transformation class including sum, string concatenation, synonym, add new columns, formatting to apply on each column 
# Exec_date, eom_transformation_var, eom_var,gcp_var are parameters assed by Airflow Python operator in deom_DAG.py
def data_transformation(exec_date, lapse_days, eom_var, gcp_var,eom_transformation_var,synonym_transformation_var,complex_synonym_transformation_var):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to transform: ", exec_date)
        eom_var = json.loads(eom_var)
        gcp_var = json.loads(gcp_var)
        transformation_var =json.loads(eom_transformation_var)
        synonym_transformation_var = json.loads(synonym_transformation_var)
        complex_synonym_transformation_var = json.loads(complex_synonym_transformation_var)
        common_ops = helpers.Common_Ops(gcp_var["project_id"], gcp_var["raw_file_bucket"], eom_var["api"], 
                                        eom_var["environment"], gcp_var["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_split_{exec_date}.pickle")
        if df.empty:
            print('no data from eom on:', exec_date)
            common_ops.save_to_pickle(df, f"eom/eom_xform_processed_{exec_date}.pickle")
        else:
            df = DataTransformation(df,transformation_var,synonym_transformation_var,complex_synonym_transformation_var).run_transformation()
            df['Channel']=np.where(df['Site']=="FACEBOOK & INSTAGRAM",'SOCIAL',df['Channel'])
            
            # Insert data to pickle
            common_ops.save_to_pickle(df, f"eom/eom_xform_processed_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise

#This function will be called when processed Data is loading into bigquery eom__reporting_processed table
# exec_date, eom_var, gcp_var are parameters passed by Airflow Python operator in deom_DAG.py
def append_bq_data(exec_date, lapse_days, eom_var, gcp_var):
    try:
        df = common_ops.get_data_from_pickle(f"eom/eom_xform_processed_{exec_date}.pickle")
        if df.empty:
            print('no data from eom on:', exec_date)
        else:  
            # Check if data exists
            if (df.shape[0]) > 0:
                dateToCheck = df['Date'][0]
                # remove the time part from timestamp
                dateToCheck = pd.to_datetime(dateToCheck).date()
                sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_table_processed"]}` where Date = "{dateToCheck}" LIMIT 1'
                df = common_ops.bq_client.query(sql).to_dataframe()
                if df.shape[0] > 0:
                    print('delete ', dateToCheck)
                    sql = f'DELETE FROM `seaocdm-data-au.{gcp_var["bq_dataset"]+common_ops.env_suffix}.{eom_var["bq_table_processed"]}` where Date = "{dateToCheck}"'
                    df = common_ops.bq_client.query(sql).to_dataframe()

            # Check column names
            api_df = common_ops.get_data_from_pickle(f"eom/eom_xform_processed_{exec_date}.pickle")        
            api_cols = api_df.columns.tolist()
        
            sql = f'SELECT column_name FROM {gcp_var["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{eom_var["bq_table_processed"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {eom_var["bq_table_processed"]} table')

            # Insert data to big query
            df = common_ops.get_data_from_pickle(f"eom/eom_xform_processed_{exec_date}.pickle")
            df = df.astype({"Placement_ID" : str})
            dataset_ref = common_ops.bq_client.dataset(gcp_var["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(eom_var["bq_table_processed"])
            table_var = bigquery.job.LoadJobVar(write_disposition='WRITE_APPEND')
            table_var.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=eom_var["location"], job_var=table_var)
            job.result()

    except:
        common_ops.send_slack_notification(eom_var["api"], datetime.datetime.now(), lapse_days)
        raise


