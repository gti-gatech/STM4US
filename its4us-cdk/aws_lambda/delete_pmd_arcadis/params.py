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
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]

        if input_params["stageVariables"]["ENV"] == "prod":
            # remove Arcadis data files from the legacy bucket
            ARCADIS_PMD_BUCKET = input_params["stageVariables"]["ARCADIS_PMD_BUCKET"] # legacy bucket
            ARCADIS_PMD_FOLDER = ""
        else:
            # remove Arcadis data files from the upload bucket and folder
            ARCADIS_PMD_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"] # internal upload bucket
            ARCADIS_PMD_FOLDER = input_params["stageVariables"]["PERFORMANCE_METRICS_FOLDER"] # e.g. performance-metrics

        query_params = input_params.get("body", {}) or {} # codepipeline places "date" in "body"

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

        if os.environ.get("STAGE") == "prod":
            # remove Arcadis data files from the legacy bucket
            ARCADIS_PMD_BUCKET = os.environ.get("ARCADIS_PMD_BUCKET") # legacy bucket
            ARCADIS_PMD_FOLDER = ""
        else:
            # remove Arcadis data files from the upload bucket and folder
            ARCADIS_PMD_BUCKET = os.environ.get("UPLOAD_BUCKET") # internal upload bucket
            ARCADIS_PMD_FOLDER = os.environ.get("PERFORMANCE_METRICS_FOLDER") # e.g. performance-metrics
    
        # set the input date to an empty string if not specified - will be set to current date later
        query_params = event.get("queryStringParameters", {}) or {}

    INPUT_DATE = query_params.get("date", "")

    if INPUT_DATE:
        print("The optional date input is specified and is", INPUT_DATE)
    else:
        print("The optional date input is not specified")

    # function-specific constants
    SURVEY_ZIPFILE = "merged.zip"
    PMD_NAMES = ["TripRequest", "OtpUser", "MonitoredTrip", "TrackedJourney", "TripSummary"]
    DATE_FORMAT_NO_DASH = "%Y%m%d"
    DATE_FORMAT_WITH_DASH = "%Y-%m-%d"
    ZIP_EXTENSION = "zip"
    TIMEZONE = "US/Eastern"

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
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "ARCADIS_PMD_BUCKET": ARCADIS_PMD_BUCKET,
        "ARCADIS_PMD_FOLDER": ARCADIS_PMD_FOLDER,
        "INPUT_DATE": INPUT_DATE,
        "SURVEY_ZIPFILE": SURVEY_ZIPFILE,
        "PMD_NAMES": PMD_NAMES,
        "DATE_FORMAT_NO_DASH": DATE_FORMAT_NO_DASH,
        "DATE_FORMAT_WITH_DASH": DATE_FORMAT_WITH_DASH,
        "ZIP_EXTENSION": ZIP_EXTENSION,
        "TIMEZONE": TIMEZONE,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS