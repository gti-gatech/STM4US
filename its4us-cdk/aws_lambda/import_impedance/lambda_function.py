import json
import requests
import boto3
import time

from params import find_params


def lambda_handler(event, context):

    # find necessary parameters
    PARAMS = find_params(event)

    # create the S3 client
    s3_client = boto3.client("s3")
    
    # copy the file on S3 root bucket to impedance folder
    copy_response = s3_client.copy_object(Bucket = PARAMS["UPLOAD_BUCKET"], 
                                          CopySource = PARAMS["ORIG_FILEPATH"],
                                          Key = PARAMS["NEW_FILEPATH"])
    print("S3 COPY File Response:", copy_response)

    # delete the old file on S3
    delete_response = s3_client.delete_object(Bucket = PARAMS["UPLOAD_BUCKET"],  
                                              Key = PARAMS["FILENAME"])
    print("S3 DELETE File Response:", delete_response)

    # POST impedance data to Neptune database
    bulkLoad_response = requests.post(PARAMS["LOADER_URL"], json=PARAMS["BULKLOAD_JSON"])
    print("Neptune POST Bulk Load Response:", bulkLoad_response.json())
    
    time.sleep(15) # wait 15 seconds before executing the search query lambda function

    # set up locations for new impedance links if needed
    print("Setting up location of the impedance links for search query as needed")
    lambda_client = boto3.client("lambda")

    invoke_response = lambda_client.invoke(FunctionName = PARAMS["SEARCH_IMPEDANCE_LAMBDA"],
                                           InvocationType = "Event",
                                           Payload = '{{ "QUERY_URL": "{}" }}'.format(PARAMS["QUERY_URL"]))

    print(invoke_response)
    print("Done setting up location on impedance links on AWS Neptune database")

    return {
        "statusCode": 200,
        "body": json.dumps("COMPLETED: IMPORT IMPEDANCE TO DATABASE")
    }