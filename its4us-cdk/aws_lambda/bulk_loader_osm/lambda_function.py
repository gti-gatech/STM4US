import json
import requests
import boto3

from params import find_params
from driver import main
from delete_data_aws import OsmAWSDataDelete


def lambda_handler(event, context):

    # find necessary parameters
    PARAMS = find_params(event)

    try:
        
        if PARAMS["METHOD"] == "POST":
            # the method processes OSM data from "osm-updated.xml" file and upload
            # the processed data to S3 bucket; no data are ingested to Neptune database.
            # Use PUT method for ingestion.
            # NOTE: OSM file too large to process without a timeout; tested on 3/20/26.
            # The output files are manually added to the S3 bucket "its4us-test-bucket" for 
            # testing purposes.
    
            # xml response from OSM API
            # osm_response = requests.get("https://api.openstreetmap.org/api/0.6/map", params={"bbox":bbox})
            # with open("/tmp/data.osm", "wb") as binary_file:
            #     binary_file.write(osm_response.content)
    
            # Get complete xml from S3
            s3 = boto3.client("s3")        
            with open(PARAMS["LAMBDA_OUTFILE"], "wb") as data:
                s3.download_fileobj(PARAMS["S3_UPLOAD_BUCKET"], PARAMS["OSM_DATAFILE"], data)
    
            # process OSM from osm-updated.xml and upload them to S3 bucket
            main(PARAMS)
    
            s3.upload_file(PARAMS["LAMBDA_NODE_OUTFILE"], PARAMS["S3_UPLOAD_BUCKET"], PARAMS["S3_NODE_OUTFILE"])
            s3.upload_file(PARAMS["LAMBDA_WAY_OUTFILE"], PARAMS["S3_UPLOAD_BUCKET"], PARAMS["S3_WAY_OUTFILE"])
            s3.upload_file(PARAMS["LAMBDA_RELATION_OUTFILE"], PARAMS["S3_UPLOAD_BUCKET"], PARAMS["S3_RELATION_OUTFILE"])
            s3.upload_file(PARAMS["LAMBDA_WAYLINK_OUTFILE"], PARAMS["S3_UPLOAD_BUCKET"], PARAMS["S3_WAYLINK_OUTFILE"])
            s3.upload_file(PARAMS["LAMBDA_RELATIONLINK_OUTFILE"], PARAMS["S3_UPLOAD_BUCKET"], PARAMS["S3_RELATIONLINK_OUTFILE"])

            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", 
                        "Access-Control-Allow-Headers": "Content-Type", 
                        "Access-Control-Allow-Origin":"*", 
                        "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
                "body": json.dumps("FINISHED: Uploaded OSM dataset to S3: {}".format(PARAMS["DATASETID"]))
            }
    
        if PARAMS["METHOD"] == "PUT":
            # the method ingests the processed OSM data from S3 to Neptune database
            
            # ingest the processed OSM data from S3 to Neptune database
            bulkLoad_response = requests.post(PARAMS["LOADER_URL"], json = PARAMS["BULKLOAD_JSON"])
    
            message = "SUCCESS: The whole process is now completed."
            print(message)
    
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", 
                        "Access-Control-Allow-Headers": "Content-Type", 
                        "Access-Control-Allow-Origin":"*", 
                        "Access-Control-Allow-Methods": "OPTIONS,PUT,POST,DELETE"},
                "body": json.dumps("FINISHED: Ingested OSM to Neptune.")
            }
        
        if PARAMS["METHOD"] == "DELETE":
            # the method deletes OSM data on the Neptune database for a given datasetid
    
            # delete OSM nodes data stored on the AWS Neptune database using datasetid
            OsmAWSDataDeleteObj = OsmAWSDataDelete(PARAMS)
            OsmAWSDataDeleteObj.create_transaction()
    
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", 
                        "Access-Control-Allow-Headers": "Content-Type", 
                        "Access-Control-Allow-Origin":"*", 
                        "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
                "body": json.dumps("FINISHED: Deleted dataset: {}".format(PARAMS["DATASETID"]))
            }
    except Exception as e:
            # convert the exception object to a string before json.dumps()
            error = str(e)
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json", 
                        "Access-Control-Allow-Headers": "Content-Type", 
                        "Access-Control-Allow-Origin":"*", 
                        "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
                "body": json.dumps(error)
            }