import io
import os
import csv
import json
from googleapiclient import discovery, http
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from google.cloud import bigquery

def header_footer_rows_count(stream, cols_req = 5):

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
def local_save(data, filename):
    jsonString = json.dumps(data)
    jsonFile = open(filename, "w")
    jsonFile.write(jsonString)
    jsonFile.close()

# deletes the temporary credential file
def local_delete(filename):
    if os.path.exists(filename):
        os.remove(filename)
    
