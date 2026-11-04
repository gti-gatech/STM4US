import json
import requests
import os
from params import find_params

def lambda_handler(event, context):
    params = find_params(event)
    
    LOAD_URL = params["LOADER_URL"]
    UPLOAD_BUCKET = params["UPLOAD_BUCKET"]
    location = params["location"]
    
    if not location:
        return {
            "statusCode": 400,
            "body": json.dumps("Missing required field 'location'")
        }
    
    bulkLoad_json = {
        "source" : f"s3://{UPLOAD_BUCKET}/{location}",
        "format" : "csv",
        "iamRoleArn" : os.environ.get("NEPTUNE_S3_ROLE_ARN"),
        "region" : os.environ.get("AWS_REGION_NAME"),
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE",
        "dependencies" : []
    }

    bulkLoad_response = requests.post(LOAD_URL, json=bulkLoad_json)
    return {
            "statusCode": 200,
            "body": bulkLoad_response.text
        }
