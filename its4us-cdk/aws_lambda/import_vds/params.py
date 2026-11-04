import os
import json
import datetime
from pytz import timezone

def find_params(event):

    PARAMS = dict()

    # determine if this is a CodePipeline run
    if "CodePipeline.job" in event.keys():

        # this is a codepipeline run
        JOB_ID = event["CodePipeline.job"]["id"]
        FUNCTION_NAME = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["FunctionName"]
        JOB_DATA = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
        input_params = json.loads(JOB_DATA)
    
        print("INPUTS:", input_params)

        PIPELINE_NAME = input_params["pipelineName"]
    
        LOADER_URL = input_params["stageVariables"]["LOADER_URL"]
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]
        NAVIGATOR_BUCKET = input_params["stageVariables"]["NAVIGATOR_BUCKET"]
        S3_UPLOAD_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"]

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        LOADER_URL = os.environ.get("LOADER_URL")
        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")
        
        # Import VDS data - use NAVIGATOR_BUCKET (legacy bucket for Navigator data)
        NAVIGATOR_BUCKET = os.environ.get("NAVIGATOR_BUCKET")
        S3_UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")

    now = datetime.datetime.now(timezone("US/Eastern"))
    filedate = now - datetime.timedelta(minutes=now.minute % 5)
    
    # function-specific constants
    VDS_DATA_FILENAME = f"vds_data_{filedate.strftime("%Y%m%dT%H%M")}.csv"

    VDS_NODE_FILENAME = "vds_nodes.csv"
    VDS_LINK_FILENAME = "vds_links.csv"
    S3_UPLOAD_FOLDER = "vds_bulk_loader"

    VDS_DEVICE_LOOKUP_FILE = "VDS_Devices_OSM_Links.csv"

    VDS_DATA_FIELDS = {
        "~id": "~id",
        "~label": "~label",
        "~from": "~from",
        "~to": "~to",
        "device_id": "device_id",
        "detector_id": "detector_id",
        "timestamp": "timestamp",
    }

    VDS = "VDS"
    LINK = "link"
    INT64_DATATYPE = "Int64"
    
    COLUMNS = {
        "device_id": "device_id:String(single)",
        "detector_id": "detector_id:String(single)",
        "timestamp": "timestamp:String(single)",
        "status": "status:String(single)",
        "volume": "volume:Int(single)",
        "speed": "speed:Float(single)",
        "occupancy": "occupancy:Int(single)",
        "confidence": "confidence:String(single)",
        "external_id": "external_id:String(single)",
        "latitude": "latitude:Float(single)",
        "longitude": "longitude:Float(single)" 
    }

    source = "s3://" + S3_UPLOAD_BUCKET + "/" + S3_UPLOAD_FOLDER
    BULKLOAD_JSON = {
        "source" : source,
        "format" : "csv",
        "iamRoleArn" : os.environ.get("NEPTUNE_S3_ROLE_ARN"),
        "region" : os.environ.get("AWS_REGION_NAME"),
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE",
        "dependencies" : []
    }

    WARNING_MESSAGE = "WARNING: Unable to find VDS file: " + VDS_DATA_FILENAME \
        + " from the S3 bucket: " + NAVIGATOR_BUCKET
    
    SUCCESS_MESSAGE = f"SUCCESS: WHOLE PROCESS IS COMPLETED FOR THE REQUEST" 

    STATUS = {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", 
                "Access-Control-Allow-Headers": "Content-Type", 
                "Access-Control-Allow-Origin":"*", 
                "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
        "body": json.dumps("STATUS MESSAGE.")
    }

    CODEPIPELINE_STATUS = {
        "jobId": JOB_ID,
        "functionName": FUNCTION_NAME,
        "pipelineName": PIPELINE_NAME,
        "status": "STATUS",
        "message": "STATUS MESSAGE.",
    }
    
    PARAMS = {
        "LOADER_URL": LOADER_URL,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "NAVIGATOR_BUCKET": NAVIGATOR_BUCKET,
        "S3_UPLOAD_BUCKET": S3_UPLOAD_BUCKET,
        "VDS_DATA_FILENAME": VDS_DATA_FILENAME,
        "VDS_NODE_FILENAME": VDS_NODE_FILENAME,
        "VDS_LINK_FILENAME": VDS_LINK_FILENAME,
        "S3_UPLOAD_FOLDER": S3_UPLOAD_FOLDER,
        "VDS_DEVICE_LOOKUP_FILE": VDS_DEVICE_LOOKUP_FILE,
        "VDS_DATA_FIELDS": VDS_DATA_FIELDS,
        "VDS": VDS,
        "LINK": LINK,
        "INT64_DATATYPE": INT64_DATATYPE,
        "COLUMNS": COLUMNS,
        "BULKLOAD_JSON": BULKLOAD_JSON,
        "WARNING_MESSAGE": WARNING_MESSAGE,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS

    