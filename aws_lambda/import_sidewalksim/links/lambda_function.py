"""
Main script driver that contains the function lambda_handler to execute AWS Lambda function 
for SidewalkSim Links data.

Currently PUT, POST and DELETE requests are supported:

    POST: Parse and upload SidewalkSim Links data to AWS Neptune database directly.
    Example: 

    DELETE: Delete SidewalkSim Links nodes from AWS Neptune database using the input parameter id.
    Example: 


"""

import boto3
import json

from query_writer_links import SidewalkSimLinksQueries
from delete_sidewalksim_data_s3 import SidewalkSimAWSDataDeleteS3


def lambda_handler(event, context):
    
    print("ALL EVENT FIELDS:", event)

    # find the request method: PUT, POST, DELETE
    env = event["stageVariables"]["env"]
    method = event["httpMethod"]

    LOADER_URL = event['stageVariables']['LOADER_URL']
    QUERY_URL = event['stageVariables']['QUERY_URL']
    SIDEWALK_BUCKET = event['stageVariables']['SIDEWALK_BUCKET']
    
    print("ENV:", env)
    print("METHOD USED:", method) # PUT, POST, DELETE
    

    if method == "PUT" or method == "POST":
        
        # extract data_set_id input parameter
        event_body = json.loads(event["body"])
        data_set_id = str(event_body["id"])

        print("DATA SET ID:", data_set_id)

        # extract filename input parameter
        filename = str(event_body["filename"]) # the csv filename on s3 to process

        print("Filename:", filename)

        # create the S3 client
        s3_client = boto3.client("s3")
        
        # copy the file on S3 root bucket to links/data_set_id folder
        bucket = SIDEWALK_BUCKET
        old_filepath = "/" + bucket + "/" + filename
        new_filepath = "links" + "/" + data_set_id + "/" + filename

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

        # process data in the csv file for create/delete nodes or links
        print("Parsing SidewalkSim Links nodes and links to AWS Neptune database")
        linksObj = SidewalkSimLinksQueries(QUERY_URL, method, SIDEWALK_BUCKET, new_filepath, data_set_id)
        linksObj.create_transaction()
        print("Done parsing SidewalkSim Links nodes and links to AWS Neptune database")


    else: # DELETE

        # extract data_set_id input parameter
        event_body = json.loads(event["body"])
        data_set_id = str(event_body["id"])

        print("DATA SET ID:", data_set_id)
    
        # delete SidewalkSim Links data stored on the AWS S3 bucket using data_set_id
        count = 0
        response_all = []
        has_objects = True
        prefix = "links" + "/" + data_set_id + "/"
        SidewalkSimAWSDataDeleteS3Obj = SidewalkSimAWSDataDeleteS3(data_set_id, SIDEWALK_BUCKET, prefix)

        print("Deleting SidewalkSim Links data files on S3 bucket")
        while has_objects:
            count += 1
            SidewalkSimAWSDataDeleteS3Obj.get_objects()
            has_objects = SidewalkSimAWSDataDeleteS3Obj.extract_objects()
            response = SidewalkSimAWSDataDeleteS3Obj.delete_objects()

            if response:
                # store the response from delete_objects()
                response_all.append(response)

            print("This is delete iteration:", count)

        # delete the folder once all objects inside the folder are deleted
        SidewalkSimAWSDataDeleteS3Obj.delete_folder()

        print("Done deleting SidewalkSim Links data files on S3 bucket")

        # delete the SidewalkSim Links nodes on the Neptune database
        print("Removing SidewalkSim Links nodes and links on AWS Neptune database by the __datasetid")
        linksObj = SidewalkSimLinksQueries(QUERY_URL, method, SIDEWALK_BUCKET, "", data_set_id)
        linksObj.create_transaction()
        print("Done removing SidewalkSim Links nodes and links on AWS Neptune database")

        if response_all:

            # print responses from delete_objects()
            print("DELETE_OBJECTS() RESPONSES:", response_all)


    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 
                    'Access-Control-Allow-Headers': 'Content-Type', 
                    'Access-Control-Allow-Origin':'*', 
                    'Access-Control-Allow-Methods': 'OPTIONS,PUT,POST,DELETE'},
        'body': json.dumps("FINISHED: The request is successfully completed.")
    }