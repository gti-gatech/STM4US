import json
import requests
from process_vds import createBulkLoadCSV
from pytz import timezone
import datetime

def lambda_handler(event, context):
    
    now = datetime.datetime.now(timezone("US/Eastern"))
    filedate = now - datetime.timedelta(minutes=now.minute % 5)
    
    # FILENAME = "vds_data_" + filedate.strftime("%Y%m%dT%H%M")
    FILENAME = event['object']
    print(FILENAME)
    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    NAVIGATOR_BUCKET = event['stageVariables']['NAVIGATOR_BUCKET']

    BULKLOAD_JSON = {
        "source" : "s3://"+NAVIGATOR_BUCKET+"/bulk_loader",
        "format" : "csv",
        "iamRoleArn" : "arn:aws:iam::760336115441:role/neptune-s3-read-access",
        "region" : "us-east-2",
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE",
        "dependencies" : []
    }

    createBulkLoadCSV(FILENAME,NAVIGATOR_BUCKET)    
        
    bulkLoad_response = requests.post(LOADER_URL, json=BULKLOAD_JSON)

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                'Access-Control-Allow-Headers': 'Content-Type', 
                'Access-Control-Allow-Origin':'*', 
                'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': json.dumps(bulkLoad_response.json())
    }
