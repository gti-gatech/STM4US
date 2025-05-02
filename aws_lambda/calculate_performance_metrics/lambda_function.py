"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for basic performance metric calculations.

Currently POST request is supported:

    POST: Calculate basic performance metrics using the data files uploaded to S3 in JSON format,
    save the computed metrics to a JSON file and upload the file to S3.

    Example: curl -v -X POST 'https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/api/pmd/calculate' 
                -H 'Content-Type:application/json' -H 'Authorization:<password>'
            
        
"""

import json

from run_compute_pmd_metrics import PMDMetricsRun


def lambda_handler(event, context):
    
    # ensure input parameters are correct
    env = event["stageVariables"]["env"] # dev or prod
    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    PUBLIC_BUCKET = event['stageVariables']['PUBLIC_BUCKET']
    PERFORMANCE_METRICS_BUCKET = event['stageVariables']['PERFORMANCE_METRICS_EXAMPLE'] # NOTE: a temporary bucket
    print("ENV:", env) # dev, prod

    # set the input week to empty string if not specified
    if event["queryStringParameters"]:
        week = event["queryStringParameters"]["week"]
        print("The optional week input is specified and is", week)
    else:
        week = ""
        print("The optional week input is not specified")

    # compute performace metrics of the STM system and upload them to S3 in json file
    print("Computing STM performance metrics and uploading the metrics to S3")
    PMDMetricsRunObj = PMDMetricsRun(env, week, PERFORMANCE_METRICS_BUCKET, QUERY_URL, PUBLIC_BUCKET)
    PMDMetricsRunObj.run_compute_pmd_metrics()
    print("Done computing STM performance metrics and uploading the metrics to S3")
    print("Whole process is completed")

    status = {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                'Access-Control-Allow-Headers': 'Content-Type', 
                'Access-Control-Allow-Origin':'*', 
                'Access-Control-Allow-Methods': 'OPTIONS,POST,DELETE'},
        'body': json.dumps("FINISHED: The request is successfully completed.")
    }

    return status
