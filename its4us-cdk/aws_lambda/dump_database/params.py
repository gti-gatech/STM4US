import os
import json

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
    
        QUERY_URL = input_params["stageVariables"]["QUERY_URL"]
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]
        PUBLIC_EXPORT_BUCKET = input_params["stageVariables"]["PUBLIC_EXPORT_BUCKET"]

        query_params = input_params.get("body", {}) or {} # codepipeline places "id" in "body"

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        QUERY_URL = os.environ.get("QUERY_URL")
        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

        # Export database dump to PUBLIC_EXPORT_BUCKET
        PUBLIC_EXPORT_BUCKET = os.environ.get("PUBLIC_EXPORT_BUCKET")
        
        query_params = event.get("queryStringParameters", {}) or {}

    DATASETID = query_params.get("id", "")

    print("DATASETID:", DATASETID)

    TRAVEL_TYPES = [ 
        'None',
        'Some',
        'Device',
        'WChairM',
        'WChairE',
        'MScooter',
        'LowVision',
        'Blind',
        'Some-LowVision',
        'Device-LowVision',
        'WChairM-LowVision',
        'WChairE-LowVision',
        'MScooter-LowVision',
        'Some-Blind',
        'Device-Blind',
        'WChairM-Blind',
        'WChairE-Blind',
        'MScooter-Blind'
    ]

    # define the output file name: one in Lambda and another in S3
    LAMBDA_SIDEWALKS_OUTFILE = "/tmp/sidewalks.csv"
    S3_SIDEWALKS_OUTFILE = "dump/sidewalks.csv"

    LAMBDA_IMPEDANCE_OUTFILE = "/tmp/baseImpedances.csv"
    S3_IMPEDANCE_OUTFILE = "dump/baseImpedances.csv"

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
        "JOB_ID": JOB_ID,
        "QUERY_URL": QUERY_URL,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "PUBLIC_EXPORT_BUCKET": PUBLIC_EXPORT_BUCKET,
        "DATASETID": DATASETID,
        "TRAVEL_TYPES": TRAVEL_TYPES,
        "LAMBDA_SIDEWALKS_OUTFILE": LAMBDA_SIDEWALKS_OUTFILE,
        "S3_SIDEWALKS_OUTFILE": S3_SIDEWALKS_OUTFILE,
        "LAMBDA_IMPEDANCE_OUTFILE": LAMBDA_IMPEDANCE_OUTFILE,
        "S3_IMPEDANCE_OUTFILE": S3_IMPEDANCE_OUTFILE,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS