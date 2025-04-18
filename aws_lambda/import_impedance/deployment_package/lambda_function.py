import json
import requests
import boto3
import time


def lambda_handler(event, context):

    # extract csv input file and env parameters
    #id = event["queryStringParameters"]["id"]
    env = str(event["queryStringParameters"]["env"])
    filename = str(event["queryStringParameters"]["filename"])

    print("EVENT:", event)
    print("ENV:", env)
    print("FILENAME:", filename)

    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    LOAD_BUCKET = event['stageVariables']['LOAD_BUCKET']
    BULKLOAD_SOURCE = "s3://"+LOAD_BUCKET

    # create the S3 client
    s3_client = boto3.client("s3")
    
    # copy the file on S3 root bucket to impedance folder
    bucket = LOAD_BUCKET
    old_filepath = "/" + bucket + "/" + filename
    new_filepath = "impedance" + "/" + filename

    response = s3_client.copy_object(
        Bucket = bucket,
        CopySource = old_filepath,
        Key = new_filepath,
    )

    # delete the old file
    response = s3_client.delete_object(
        Bucket = bucket,
        Key = filename,
    )

    bulkLoad_json = {
            "source" : BULKLOAD_SOURCE+"/impedance",
            "format" : "csv",
            "iamRoleArn" : "arn:aws:iam::760336115441:role/neptune-s3-read-access",
            "region" : "us-east-2",
            "failOnError" : "FALSE",
            "parallelism" : "MEDIUM",
            "updateSingleCardinalityProperties" : "TRUE",
            "queueRequest" : "TRUE",
            "dependencies" : []
        }

    bulkLoad_response = requests.post(LOADER_URL, json=bulkLoad_json)

    print("Bulk Load Response:", bulkLoad_response.json())
    
    time.sleep(15) # wait 15 seconds before executing the search query lambda function

    # set up locations for new impedance links if needed
    print("Setting up location of the impedance links for search query as needed")
    lambda_client = boto3.client("lambda")

    invoke_response = lambda_client.invoke(FunctionName="search-query-impedance",
                                           InvocationType="Event",
                                           Payload='{{ "env": "{}" }}'.format(env))
    print(invoke_response)
    print("Done setting up location on impedance links on AWS Neptune database")

    return {
        "statusCode": 200,
        "body": bulkLoad_response.json()
    }