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
from data_transformation import DataTransformation


def count_header_footer_rows(stream, cols_req = 5):
    """
    counts the headers and footers in a csv report
    :param stream: csv file stream
    :type stream: stream
    :param cols_req: the number of cols for it to register as actual data
    :type cols_req: int
    :return: [count_of_headers, count_of_footers]
    :rtype: [int, int]
    """
    num_rows = {'head_count': 0, 'body_count': 0, 'footer_count': 0}
    curr_key = 'head_count'

    for line in csv.reader(stream):
        if len(line) >= cols_req and curr_key in ['head_count', 'body_count']:
            curr_key = 'body_count'
        elif len(line) < cols_req and curr_key == 'body_count':
            curr_key = 'footer_count'
        num_rows[curr_key] += 1

    return num_rows['head_count'], num_rows['footer_count']

# used for temporary saving of credentials
def save_to_local(data, filename):
    jsonString = json.dumps(data)
    jsonFile = open(filename, "w")
    jsonFile.write(jsonString)
    jsonFile.close()

# deletes the temporary credential file
def delete_local_file(filename):
    if os.path.exists(filename):
        os.remove(filename)
    
# Data fetching function by API calls. The 1st task of the DAG. 
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def fetch(exec_date, lapse_days, cm_config, gcp_config):    
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to fetch: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        secrets =  json.loads(common_ops.access_secret_version("CHEP_CM_CLIENT_SECRETS"))
        secret_file = 'secrets.json'
        save_to_local(secrets, secret_file)
        
        token = json.loads(common_ops.access_secret_version("CHEP_CM_CREDS_ACCESS_TOKEN"))
        token_file = 'token.json'
        save_to_local(token, token_file)
        
        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(secret_file, scope=cm_config["api_scopes"])
        
        # Check whether credentials exist in the credential store. Using a credential
        # store allows auth credentials to be cached, so they survive multiple runs
        # of the application. This avoids prompting the user for authorization every
        # time the access token expires, by remembering the refresh token.
        storage = oauthFile.Storage(token_file)
        print("Storage: ",storage)
        
        credentials = storage.get()
        
        args = argparser.parse_args(args=[])
        args.noauth_local_webserver = True
        print("TOOLS: ",tools.argparser.parse_known_args()[0])
        print("ARGS: ", args)

        # # If no credentials were found, go through the authorization process and
        # # persist credentials to the credential store.
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, args)

        print("Trying to authorize..")
        # Use the credentials to authorize an httplib2.Http instance.
        http_oauth = credentials.authorize(httplib2.Http())

        # # Construct a service object via the discovery service.
        service = discovery.build(cm_config["dcm_api_name"], cm_config["api_version"], http=http_oauth)
        start_date = datetime.datetime.strptime(exec_date, '%Y-%m-%d') 
        end_date = start_date #+ timedelta(days=1)

        delete_local_file('secrets.json')
        delete_local_file('token.json')

        report = {
            # Set the required fields "name" and "type".
            'name': 'CM Reporting Raw',
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
        # # Add the criteria to the report resource.
        report['criteria'] = criteria

        # Update an existing report.
        DCM_PROFILE_ID = common_ops.access_secret_version("CHEP_CM_PROFILE_ID")
        REPORT_ID = common_ops.access_secret_version("CHEP_CM_REPORT_ID")
        patched_report = service.reports().patch(profileId=DCM_PROFILE_ID, reportId=REPORT_ID, body=report).execute()
        report_run = service.reports().run(profileId=DCM_PROFILE_ID, reportId=REPORT_ID).execute()

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
        RAW_FILE = f'dcm_raw{exec_date}.csv'
        delete_local_file(RAW_FILE)
        if report_file['status'] == 'REPORT_AVAILABLE':
            # Prepare a local file to download the report contents to.
            out_file = io.FileIO(RAW_FILE, mode='wb')

            # Create a get request.
            request = service.files().get_media(reportId=REPORT_ID, fileId=file_id)

            # Create a media downloader instance.
            downloader = http.MediaIoBaseDownload(out_file, request)

            # Execute the get request and download the file.
            download_finished = False
            while download_finished is False:
                _, download_finished = downloader.next_chunk()

            print('File %s downloaded to %s' % (file_id, os.path.realpath(out_file.name)))
        
        with open(RAW_FILE, 'r') as f2:
            data = f2.read()
        output = io.StringIO(data)
        columns_to_skip = 5
        header_rows, footer_rows = count_header_footer_rows(output, columns_to_skip)
        footer_rows += 1 
        f2.close()
        df = pd.read_csv(RAW_FILE, skiprows=header_rows, skipfooter=footer_rows, engine='python')
        #API endpoints no data situation handling 
        if (df.shape[0])>0:
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_raw_{exec_date}.pickle")
        
        # this condition happens when dataframe is empty and slack_notification_if_nodata is "TRUE" so we can send slack notification
        elif df.empty and cm_config["slack_notification_if_nodata"] =="TRUE":
            common_ops.send_slack_notification("function fetch ran for datasource campaign manager no data was returned for the requested day: ",exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_raw_{exec_date}.pickle")
        
        # this condition happens when dataframe is empty and slack_notification_if_nodata is "FALSE" so we can switch off the slack notification options
        else:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_raw_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# Data fetching function by API calls
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def cid_fetch(exec_date, lapse_days, cm_config, gcp_config):    
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to fetch: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        secrets =  json.loads(common_ops.access_secret_version("CHEP_CM_CLIENT_SECRETS"))
        secret_file = 'secrets.json'
        save_to_local(secrets, secret_file)
        
        token = json.loads(common_ops.access_secret_version("CHEP_CM_CREDS_ACCESS_TOKEN"))
        token_file = 'token.json'
        save_to_local(token, token_file)
        
        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(secret_file, scope=cm_config["api_scopes"])
        
        # Check whether credentials exist in the credential store. Using a credential
        # store allows auth credentials to be cached, so they survive multiple runs
        # of the application. This avoids prompting the user for authorization every
        # time the access token expires, by remembering the refresh token.
        storage = oauthFile.Storage(token_file)
        print("Storage: ",storage)
        
        credentials = storage.get()
        
        args = argparser.parse_args(args=[])
        args.noauth_local_webserver = True
        print("TOOLS: ",tools.argparser.parse_known_args()[0])
        print("ARGS: ", args)

        # # If no credentials were found, go through the authorization process and
        # # persist credentials to the credential store.
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, args)

        print("Trying to authorize..")
        # Use the credentials to authorize an httplib2.Http instance.
        http_oauth = credentials.authorize(httplib2.Http())

        # # Construct a service object via the discovery service.
        service = discovery.build(cm_config["dcm_api_name"], cm_config["api_version"], http=http_oauth)
        start_date = datetime.datetime.strptime(exec_date, '%Y-%m-%d') 
        end_date = start_date #+ timedelta(days=1)

        delete_local_file('secrets.json')
        delete_local_file('token.json')

        report = {
            # Set the required fields "name" and "type".
            'name': 'CM CID Reporting Raw',
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
        # # Add the criteria to the report resource.
        report['criteria'] = criteria

        # Update an existing report.
        DCM_PROFILE_ID = common_ops.access_secret_version("CHEP_CM_PROFILE_ID")
        REPORT_ID = common_ops.access_secret_version("CHEP_CM_CID_REPORT_ID")
        patched_report = service.reports().patch(profileId=DCM_PROFILE_ID, reportId=REPORT_ID, body=report).execute()
        report_run = service.reports().run(profileId=DCM_PROFILE_ID, reportId=REPORT_ID).execute()

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
        RAW_FILE = f'dcm_raw{exec_date}.csv'
        delete_local_file(RAW_FILE)
        if report_file['status'] == 'REPORT_AVAILABLE':
            # Prepare a local file to download the report contents to.
            out_file = io.FileIO(RAW_FILE, mode='wb')

            # Create a get request.
            request = service.files().get_media(reportId=REPORT_ID, fileId=file_id)

            # Create a media downloader instance.
            downloader = http.MediaIoBaseDownload(out_file, request)

            # Execute the get request and download the file.
            download_finished = False
            while download_finished is False:
                _, download_finished = downloader.next_chunk()

            print('File %s downloaded to %s' % (file_id, os.path.realpath(out_file.name)))
        
        with open(RAW_FILE, 'r') as f2:
            data = f2.read()
        output = io.StringIO(data)
        columns_to_skip = 5
        header_rows, footer_rows = count_header_footer_rows(output, columns_to_skip)
        footer_rows += 1 
        f2.close()
        df = pd.read_csv(RAW_FILE, skiprows=header_rows, skipfooter=footer_rows, engine='python')
        #API endpoints no data situation handling 
        if (df.shape[0])>0:
            df["CID"] = df["Landing Page URL"].str.extract(r'(cid.*?)&')
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_cid_raw_{exec_date}.pickle")
        
        # this condition happens when dataframe is empty and slack_notification_if_nodata is "TRUE" so we can send slack notification
        elif df.empty and cm_config["slack_notification_if_nodata"] =="TRUE":
            common_ops.send_slack_notification("function fetch ran for datasource campaign manager cid no data was returned for the requested day: ",exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_cid_raw_{exec_date}.pickle")
        
        # this condition happens when dataframe is empty and slack_notification_if_nodata is "FALSE" so we can switch off the slack notification options
        else:
            print('no data from campaign manager cid on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_cid_raw_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise
    
# Data transformation function. Called by campaign_manager_xform task to clean and transform raw data from fetch task.
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def data_xform(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to transform: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_raw_{exec_date}.pickle")
        cid_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_cid_raw_{exec_date}.pickle")
        if df.empty:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_{exec_date}.pickle")
        elif len(df) > 0:
            df.columns = df.columns.str.replace('\(|\s+|\)|:|-|\[|\]|%|\.', '_', regex=True)
            df.rename(columns={"Site_ID__CM360_": "Site_ID__DCM_", "Site__CM360_":"Site__DCM_", "DV360_Cost__Account_Currency_": "DBM_Cost__Account_Currency_"}, inplace=True)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_{exec_date}.pickle")
        if len(cid_df) > 0:
            cid_df.columns = cid_df.columns.str.replace('\(|\s+|\)|:|-|\[|\]|%|\.', '_', regex=True)
            common_ops.save_to_pickle(cid_df, f"campaign_manager/campaign_manager_cid_xform_{exec_date}.pickle")
        elif cid_df.empty:
            print('no data from campaign manager cid on:', exec_date)
            common_ops.save_to_pickle(cid_df, f"campaign_manager/campaign_manager_cid_xform_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# Check if current data exist in Big Query Table. exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def check_data_exist(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to date check: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config) 
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_{exec_date}.pickle")
        cid_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_cid_xform_{exec_date}.pickle")
        if (df.shape[0]) > 0:
            dateToCheck = df['Date'][0]
            sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table"]}` where Date = "{dateToCheck}" LIMIT 1'
            df = common_ops.bq_client.query(sql).to_dataframe()
            if df.shape[0] > 0:
                print('delete ', dateToCheck)
                sql = f'DELETE FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table"]}` where Date = "{dateToCheck}"'
                df = common_ops.bq_client.query(sql).to_dataframe()
        elif (df.shape[0]) < 0:
            print('no data from campaign manager on:', exec_date)

        if (cid_df.shape[0]) > 0:
            dateToCheck = cid_df['Date'][0]
            sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_cid_table"]}` where Date = "{dateToCheck}" LIMIT 1'
            cid_df = common_ops.bq_client.query(sql).to_dataframe()
            if cid_df.shape[0] > 0:
                print('delete ', dateToCheck)
                sql = f'DELETE FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_cid_table"]}` where Date = "{dateToCheck}"'
                cid_df = common_ops.bq_client.query(sql).to_dataframe()        
        elif (cid_df.shape[0]) < 0:
            print('no data from campaign manager cid on:', exec_date)
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# Check API column names matches Big Query Table. exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def check_column_names(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to column check: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        api_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_{exec_date}.pickle")
        api_cid_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_cid_xform_{exec_date}.pickle")
        if (api_df.shape[0]) == 0:
             print('no data from campaign manager on:', exec_date)
        elif (api_df.shape[0]) > 0:
            api_cols = api_df.columns.tolist()
            sql = f'SELECT column_name FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{cm_config["bq_table"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {cm_config["bq_table"]} table')  
        
        if (api_cid_df.shape[0]) == 0:
             print('no data from campaign manager cid on:', exec_date)
        elif (api_cid_df.shape[0]) > 0:    
            api_cid_cols = api_cid_df.columns.tolist()
            sql = f'SELECT column_name FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{cm_config["bq_cid_table"]}"'
            cid_df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = cid_df.column_name.tolist()

            mismatch = set(api_cid_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {cm_config["bq_cid_table"]} table')
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise
# Data loading function to be called by campaign_manager task. Read the transformed data in binary file from binary file in disk
# and append to Big Query raw data table. exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def data_load(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_{exec_date}.pickle")
        cid_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_cid_xform_{exec_date}.pickle")
        
        if df.empty:
            print('no data from campaign manager on:', exec_date)
        elif len(df) > 0:
        # Insert data to big query
            dataset_ref = common_ops.bq_client.dataset(gcp_config["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(cm_config["bq_table"])
            table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
            table_config.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=cm_config["location"], job_config=table_config)
            job.result() 

        if cid_df.empty:
            print('no data from campaign manager cid on:', exec_date)
        elif len(cid_df) > 0:
        # Insert data to big query
            dataset_ref = common_ops.bq_client.dataset(gcp_config["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(cm_config["bq_cid_table"])
            table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
            table_config.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(cid_df, table_ref, location=cm_config["location"], job_config=table_config)
            job.result()  
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# Data split exception function to be called by cm task. Read the transformed data in binary file from binary file in disk
# solve complication for cm360, look up campaigns not need and join placement_cost_split table
#exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py#
def data_split_exception(exec_date, lapse_days, cm_config, gcp_config,cm_split_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        cm_split_config = json.loads(cm_split_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"],
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_{exec_date}.pickle")
        if df.empty:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_exception_{exec_date}.pickle")
        else:

            ########################## Start of crappy code ########################
            # Reason for crappy code: unlike other data points (Facebook/Amobee/Snapchat/etc.), there are some complications when it comes to CM360
            # Apart from merely key-value splitting, the original SQL script cm__reporting_raw__split is doing some other data processing tasks to create some additional columns:
            # This code is ad-hoc for CM360 only

            # Remove measures that have been collected directly from APIs (Facebook/Twitter/Amobee/Snapchat) to avoid double counting
            # Measures include: Impressions, Clicks, Video_plays and Video_completions
            sites_to_exclude = cm_split_config["sites_to_exclude"]
            measures_to_exclude = cm_split_config["measures_to_exclude"]

            # If Site__DCM_ column contains sites in the sites_to_exclude_list, then set the value to 0, otherwise keep the original values
            for key, value in sites_to_exclude.items():
                column = key
                sites_to_exclude_list = [i.lower() for i in value] # lower case the whole list

                # For each measure, apply the rule to replace 0 where Site is in site_to_exclude
                for measure in measures_to_exclude:
                    df[measure] = np.where(df[column].str.lower().isin(sites_to_exclude_list), 0, df[measure])
            
            # Calculation of Click Through / View Through Conversions when Activity_ID = 10151249
            # marketing_visit_ct
            df["Marketing_Visit_CT"] = np.where(df["Activity_ID"]=="10151249",df["Click_through_Conversions"],0)
            # marketing_visit_vt
            df["Marketing_Visit_VT"] = np.where(df["Activity_ID"]=="10151249",df["View_through_Conversions"],0)
            # marketing_visit_total
            df["Marketing_Visit_Total"] = np.where(df["Activity_ID"]=="10151249",df["Total_Conversions"],0)

            # To add Intent columns based on values of Activity column
            # Columns include: intent_ct, intent_vt, intent_total, im_warm_intent, im_hot_intent, im_intent_total, ce_warm_intent, ce_hot_intent, ce_intent_total

            #  IM Warm/Hot Intent 2021 is applied from March 1st 2021 onwards
            activities_list = ["im-warm-intent-2021", "im-hot-intent-2021", "ce-warm-intent", "ce-hot-intent"]

            # Intent_CT
            df["Intent_CT"] = np.where(df["Activity"].str.contains("|".join(activities_list), case=False),df["Click_through_Conversions"],0)

            # Intent_VT
            df["Intent_VT"] = np.where(df["Activity"].str.contains("|".join(activities_list), case=False),df["View_through_Conversions"],0)

            # IM Warm/Hot/Total Intent
            df["IM_Warm_Intent"] = df["Total_Conversions"][df["Activity_ID"] == "10839024"]
            df["IM_Hot_Intent"] = df["Total_Conversions"][df["Activity_ID"] == "10804256"]
            df["IM_Intent_Total"] = df["IM_Warm_Intent"].fillna(0) + df["IM_Hot_Intent"].fillna(0)

            # CE Warm/Hot/Total Intent
            df["CE_Warm_Intent"] = df["Total_Conversions"][df["Activity_ID"].isin([
                "10095052",
                "10093033",
                "10177055",
                "10094764",
                "10578745",
                "11006964",
                "10087516",
                "11008923",
                "10094437",
                "10578640",
                "10063034",
                "10059071",
                "10058744",
                "10087519",
                "10092642",
                "10087510",
                "10095055",
                "10092304",
                "10217164"
                ])]

            df["CE_Hot_Intent"] = df["Total_Conversions"][df["Activity_ID"] == "9573164"]
            df["CE_Intent_Total"] = df["CE_Warm_Intent"].fillna(0) + df["CE_Hot_Intent"].fillna(0)
            
            # Total Intent
            df["Intent"] = df["IM_Warm_Intent"].fillna(0) + df["IM_Hot_Intent"].fillna(0) + df["CE_Warm_Intent"].fillna(0) + df["CE_Hot_Intent"].fillna(0)
            df["Intent_Total"] = df["Intent"]
            ########################## End of crappy code ################################
            
            # look up campaigns to exclude
            exclude_campaigns_list = cm_split_config["exclude_campaigns_list"]
            df = df.drop(df[df['Campaign_ID'].isin(exclude_campaigns_list)].index, axis=0,inplace=False)
            
            # join Placement_cost table
            sql_placement_costs=f'SELECT * FROM seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_placement_costs"]}'
            df_placement_costs = common_ops.bq_client.query(sql_placement_costs).to_dataframe()
            df['Date'] =df['Date'].astype('str')
            df['Placement_ID'] =df['Placement_ID'].astype('str')
            df_placement_costs['dates']=df_placement_costs['dates'].astype('str')
            df_placement_costs['placement_ids']=df_placement_costs['placement_ids'].astype('str')
            df = df.merge(df_placement_costs, how='left', left_on=['Date','Placement_ID'],right_on= ['dates','placement_ids'])
       
            # Insert data to pickle
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_exception_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# Data joining function to be called by cm task. Read the transformed data in binary file from binary file in disk
# Extract cid from cm cid reports
# and join with cm reports
def data_cid_combine(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"],
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_exception_{exec_date}.pickle")

        if df.empty:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_cid_xform_combine_{exec_date}.pickle")
        else:
            sql = f'SELECT * FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_cid_lookup"]}'
            cid_df = common_ops.bq_client.query(sql).to_dataframe()
            # Rename columns and remove unecessary values
            cid_df = cid_df.rename(columns = {"Site_ID__CM360_" : "Site_ID__DCM_"})
            cid_df = cid_df.reset_index(drop = True)
            # Get only cid ending in '_pfm' if there are multiple cids for one day
            cid_df_count = cid_df.copy()
            cid_df_count['Count'] = cid_df_count.groupby(["Creative_ID", "Advertiser_ID", "Campaign_ID", "Site_ID__DCM_", "Placement_ID", "Ad_ID"])['CID'].transform('count')
            cid_df_count = cid_df_count[cid_df_count["Count"] > 1]
            removed_cid_index = cid_df_count.index.values.tolist()
            removed_cid_index_list = []
            for i in removed_cid_index:
                if not cid_df["CID"].iloc[i].endswith("pfm"):
                    removed_cid_index_list.append(i)
            cid_df = cid_df.drop(cid_df.index[removed_cid_index_list])

            #join cid df to df
            for col in ["Creative_ID", "Placement_ID", "Advertiser_ID", "Campaign_ID", "Site_ID__DCM_", "Ad_ID"]:
                cid_df[col] = cid_df[col].apply(lambda i: str(i))
                df[col] = df[col].apply(lambda i: str(i))
            
            df = df.merge(cid_df, on = ["Creative_ID", "Advertiser_ID", "Campaign_ID", "Site_ID__DCM_", "Placement_ID", "Ad_ID"], how = "left")

            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_cid_xform_combine_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# Data loading function to be called by cm task. Read the transformed data in binary file from binary file in disk
# Configure data transformation using keyvalue_split_func
def data_split(exec_date, lapse_days, cm_config, gcp_config, cm_split_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        cm_split_config = json.loads(cm_split_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"],
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_cid_xform_combine_{exec_date}.pickle")
        if df.empty:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_split_{exec_date}.pickle")
        else:
            # Apply Key Value Split Transformation
            df = Split(df,cm_split_config)
            # Insert data to pickle
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_split_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# This function will be called when split data is loading into bigquery cm__reporting_raw__split table
# It will check final table column name and data exist first, then appending to Big Query split data table
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def append_bq_split_data(exec_date, lapse_days, cm_config, cm_split_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        cm_split_config = json.loads(cm_split_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        # Check if data exists
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_split_{exec_date}.pickle")
        
        if (df.shape[0]) > 0:
            dateToCheck = df['DATE'][0]
            # remove the time part from timestamp
            # dateToCheck = pd.to_datetime(dateToCheck).date()
            sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_split_table"]}` where Date = "{dateToCheck}" LIMIT 1'
            df = common_ops.bq_client.query(sql).to_dataframe()
            if df.shape[0] > 0:
                print('delete ', dateToCheck)
                sql = f'DELETE FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_split_table"]}` where Date = "{dateToCheck}"'
                df = common_ops.bq_client.query(sql).to_dataframe()

            # Check column names
            api_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_split_{exec_date}.pickle")  
            api_df = api_df[cm_split_config["selected_columns"]]      
            api_cols = api_df.columns.tolist()
            api_cols = [col.lower() for col in api_cols]
        
            sql = f'SELECT column_name FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{cm_config["bq_split_table"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()
            table_columns = [col.lower() for col in table_columns]

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {cm_config["bq_split_table"]} table')

            # Insert data to big query
            df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_split_{exec_date}.pickle")
            df = df[cm_split_config["selected_columns"]]
            dataset_ref = common_ops.bq_client.dataset(gcp_config["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(cm_config["bq_split_table"])
            table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
            table_config.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=cm_config["location"], job_config=table_config)
            job.result()
        
        else:
            print('no data from campaign manager on:', exec_date)

    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

#Implement data transformation class including sum, string concatination, synonym, add new columns, formatting to apply on each column 
# Exec_date, cm_transformation_config, cm_config,gcp_config are parameters assed by Airflow Python operator in dcm_DAG.py
def data_transformation(exec_date, lapse_days, cm_config, gcp_config,cm_transformation_config,synonym_transformation_config,complex_synonym_transformation_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to transform: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        transformation_config =json.loads(cm_transformation_config)
        synonym_transformation_config = json.loads(synonym_transformation_config)
        complex_synonym_transformation_config = json.loads(complex_synonym_transformation_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_split_{exec_date}.pickle")
        if df.empty:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")
        else:
            df = DataTransformation(df,transformation_config,synonym_transformation_config,complex_synonym_transformation_config).run_transformation()
            df['Channel']=np.where(df['Site']=="FACEBOOK & INSTAGRAM",'SOCIAL',df['Channel'])
            
            # Insert data to pickle
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")
    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

#This function will be called when processed Data is loading into bigquery cm__reporting_processed table
#It will check final table column name and data exist first, then appending to Big Query processed data table. 
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def append_bq_data(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")
        if df.empty:
            print('no data from campaign manager on:', exec_date)
        else:  
            # Check if data exists
            if (df.shape[0]) > 0:
                dateToCheck = df['Date'][0]
                # remove the time part from timestamp
                dateToCheck = pd.to_datetime(dateToCheck).date()
                sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_processed"]}` where Date = "{dateToCheck}" LIMIT 1'
                df = common_ops.bq_client.query(sql).to_dataframe()
                if df.shape[0] > 0:
                    print('delete ', dateToCheck)
                    sql = f'DELETE FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_processed"]}` where Date = "{dateToCheck}"'
                    df = common_ops.bq_client.query(sql).to_dataframe()

            # Check column names
            api_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")        
            api_cols = api_df.columns.tolist()
        
            sql = f'SELECT column_name FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{cm_config["bq_table_processed"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {cm_config["bq_table_processed"]} table')

            # Insert data to big query
            df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")
            df = df.astype({"Placement_ID" : str})
            dataset_ref = common_ops.bq_client.dataset(gcp_config["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(cm_config["bq_table_processed"])
            table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
            table_config.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=cm_config["location"], job_config=table_config)
            job.result()

    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

#This function will be called when Data in processed table is loading into bigquery all__combined_reporting table
#It will check combined table column name and data exist first, then appending to Big Query all__combined_reporting table. 
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def data_combined(exec_date, lapse_days, cm_config, gcp_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")
        if df.empty:
            print('no data from campaign manager on:', exec_date)
        else:  
            # Check if data exists
            if (df.shape[0]) > 0:
                dateToCheck = df['Date'][0]
                # remove the time part from timestamp
                dateToCheck = pd.to_datetime(dateToCheck).date()
                sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_combined"]}` where Date = "{dateToCheck}" and Downloaded_From = "DCM Basic" LIMIT 1'
                df = common_ops.bq_client.query(sql).to_dataframe()
                if df.shape[0] > 0:
                    print('delete ', dateToCheck)
                    sql = f'DELETE FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_combined"]}` where Date = "{dateToCheck}" and Downloaded_From = "DCM Basic"'
                    df = common_ops.bq_client.query(sql).to_dataframe()

            # Check column names
            api_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")        
            api_cols = api_df.columns.tolist()
        
            sql = f'SELECT column_name FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{cm_config["bq_table_combined"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {cm_config["bq_table_combined"]} table')

            # Insert data to big query
            df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")
            df = df.astype({"Placement_ID" : str})
            dataset_ref = common_ops.bq_client.dataset(gcp_config["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(cm_config["bq_table_combined"])
            table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
            table_config.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=cm_config["location"], job_config=table_config)
            job.result()

    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise

# This function will be called when Data in processed table is loading into bigquery all__combined_reporting_mini table
# It will check combined table column name and data exist first, then appending to Big Query all__combined_reporting_mini table. 
# exec_date, cm_config, gcp_config are parameters passed by Airflow Python operator in dcm_DAG.py
def data_combined_mini(exec_date, lapse_days, cm_config, gcp_config, cm_transformation_mini_config, synonym_transformation_config, complex_synonym_transformation_config):
    try:
        exec_date = (datetime.datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(days=lapse_days)).strftime('%Y-%m-%d') # Plus 1 day to set exec date to the end of the interval
        print("date of data to load: ", exec_date)
        cm_config = json.loads(cm_config)
        gcp_config = json.loads(gcp_config)
        cm_transformation_mini_config = json.loads(cm_transformation_mini_config)
        synonym_transformation_config = json.loads(synonym_transformation_config)
        complex_synonym_transformation_config = json.loads(complex_synonym_transformation_config)
        common_ops = helpers.Common_Ops(gcp_config["project_id"], gcp_config["raw_file_bucket"], cm_config["api"], 
                                        cm_config["environment"], gcp_config["gcp_cred_file_path"])
        df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_xform_processed_{exec_date}.pickle")

        if df.empty:
            print('no data from campaign manager on:', exec_date)
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_processed_mini_{exec_date}.pickle")
        else:  
            # Apply Data Transformation
            df = DataTransformation(df, cm_transformation_mini_config, synonym_transformation_config, complex_synonym_transformation_config).run_transformation()
            common_ops.save_to_pickle(df, f"campaign_manager/campaign_manager_processed_mini_{exec_date}.pickle")

            # Check if data exists
            if (df.shape[0]) > 0:
                dateToCheck = df['Date'][0]
                # remove the time part from timestamp
                dateToCheck = pd.to_datetime(dateToCheck).date()
                sql = f'SELECT Date FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_combined_mini"]}` where Date = "{dateToCheck}" and Downloaded_From = "DCM Basic" LIMIT 1'
                df = common_ops.bq_client.query(sql).to_dataframe()
                if df.shape[0] > 0:
                    print('delete ', dateToCheck)
                    sql = f'DELETE FROM `seaocdm-data-au.{gcp_config["bq_dataset"]+common_ops.env_suffix}.{cm_config["bq_table_combined_mini"]}` where Date = "{dateToCheck}" and Downloaded_From = "DCM Basic"'
                    df = common_ops.bq_client.query(sql).to_dataframe()

            # Check column names
            api_df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_processed_mini_{exec_date}.pickle")        
            api_cols = api_df.columns.tolist()
        
            sql = f'SELECT column_name FROM {gcp_config["bq_dataset"]+common_ops.env_suffix}.INFORMATION_SCHEMA.COLUMNS where table_name = "{cm_config["bq_table_combined_mini"]}"'
            df = common_ops.bq_client.query(sql).to_dataframe()
            table_columns = df.column_name.tolist()

            mismatch = set(api_cols) - set(table_columns)
            if len(mismatch) > 0:
                raise Exception(f'Mismatch columns. API field/s {mismatch} not defined in {cm_config["bq_table_combined_mini"]} table')

            # Insert data to big query
            df = common_ops.get_data_from_pickle(f"campaign_manager/campaign_manager_processed_mini_{exec_date}.pickle")
            dataset_ref = common_ops.bq_client.dataset(gcp_config["bq_dataset"]+common_ops.env_suffix)
            table_ref = dataset_ref.table(cm_config["bq_table_combined_mini"])
            table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
            table_config.autodetect = True
            job = common_ops.bq_client.load_table_from_dataframe(df, table_ref, location=cm_config["location"], job_config=table_config)
            job.result()

    except:
        common_ops.send_slack_notification(cm_config["api"], datetime.datetime.now(), lapse_days)
        raise