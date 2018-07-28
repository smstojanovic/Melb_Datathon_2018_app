import boto3
import numpy as np
import pandas as pd
import time
from io import StringIO

# DEBUG
# query = """SELECT *
# FROM default.datathon_transactions
# limit 100;"""
#
# database = "default"
# s3_output = "s3://aws-athena-query-results-269312820650-ap-southeast-2/"

def query(
    query,
    database = "default",
    s3_output = "s3://aws-athena-query-results-269312820650-ap-southeast-2/"):

    session = boto3.Session(profile_name='admin')
    athena = session.client('athena',region_name='ap-southeast-2')

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    # print('Execution ID: ' + response['QueryExecutionId'])

    query_id = response['QueryExecutionId']

    query_state = 'RUNNING'

    while query_state == 'RUNNING':
        query_results = athena.get_query_execution(QueryExecutionId=query_id)
        query_state = query_results['QueryExecution']['Status']['State']
        time.sleep(1)

    s3 = session.client('s3',region_name='ap-southeast-2')

    try:
        s3.head_object(
            Bucket='aws-athena-query-results-269312820650-ap-southeast-2',
            Key= query_id + '.csv'
        )
        # print('file found')
    except:
        raise FileNotFoundError
        # print('file not found')

    s3 = session.resource('s3',region_name='ap-southeast-2')

    obj = s3.Object(
        'aws-athena-query-results-269312820650-ap-southeast-2',
        query_id + '.csv'
    )

    file_string = StringIO(obj.get()['Body'].read().decode())

    df = pd.read_csv(file_string)
    del file_string

    return df
