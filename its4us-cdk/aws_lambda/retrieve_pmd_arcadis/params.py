import os
import json
import boto3

_secretsmanager = boto3.client("secretsmanager")
_secret_cache = {}


def _get_arcadis_api_key():
    secret_id = os.environ["ARCADIS_API_KEY_SECRET_ID"]
    if secret_id not in _secret_cache:
        response = _secretsmanager.get_secret_value(SecretId=secret_id)
        value = response.get("SecretString")
        if not value:
            raise RuntimeError("Configured Arcadis API key secret has no value")
        _secret_cache[secret_id] = value
    return _secret_cache[secret_id]

def find_params(event):

    PARAMS = dict()

    # PARAMETERS
    # determine if this is a CodePipeline run
    if "CodePipeline.job" in event.keys():
        
        # this is a codepipeline run
        JOB_ID = event["CodePipeline.job"]["id"]
        FUNCTION_NAME = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["FunctionName"]
        JOB_DATA = event["CodePipeline.job"]["data"]["actionConfiguration"]["configuration"]["UserParameters"]
        input_params = json.loads(JOB_DATA)
    
        PIPELINE_NAME = input_params["pipelineName"]
        ADMIN_LOG_BUCKET = input_params["stageVariables"]["ADMIN_LOG_BUCKET"]

        if input_params["stageVariables"]["ENV"] == "prod":
            # upload Arcadis data to legacy bucket
            S3_UPLOAD_BUCKET = input_params["stageVariables"]["ARCADIS_PMD_BUCKET"] # legacy bucket
            S3_UPLOAD_FOLDER = ""
        else:
            # upload Arcadis data to upload bucket and folder
            S3_UPLOAD_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"]
            S3_UPLOAD_FOLDER = input_params["stageVariables"]["PERFORMANCE_METRICS_FOLDER"]

        ARCADIS_API_KEY = _get_arcadis_api_key()

        query_params = input_params.get("body", {}) or {} # codepipeline places "date" in "body"

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

        if os.environ.get("STAGE") == "prod":
            # upload Arcadis data to legacy bucket
            S3_UPLOAD_BUCKET = os.environ.get("ARCADIS_PMD_BUCKET") # legacy bucket
            S3_UPLOAD_FOLDER = ""
        else:
            # upload Arcadis data to upload bucket and folder
            S3_UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")
            S3_UPLOAD_FOLDER = os.environ.get("PERFORMANCE_METRICS_FOLDER")

        # set the input date to an empty string if not specified - will be set to current date later
        query_params = event.get("queryStringParameters", {}) or {}

        ARCADIS_API_KEY = _get_arcadis_api_key()

    INPUT_DATE = query_params.get("date", "")

    if INPUT_DATE:
        print("The optional date input is specified and is", INPUT_DATE)
    else:
        print("The optional date input is not specified")

    
    # specify the root URL for retrieving PMD data from the configured provider
    URL_ROOT = os.environ["ARCADIS_API_URL_ROOT"]

    # function-specific constants
    TRIP_SURVEY = "trip-survey-responses"
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
        "INPUT_DATE": INPUT_DATE,
        "ARCADIS_API_KEY": ARCADIS_API_KEY,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "S3_UPLOAD_BUCKET": S3_UPLOAD_BUCKET,
        "S3_UPLOAD_FOLDER": S3_UPLOAD_FOLDER,
        "URL_ROOT": URL_ROOT,
        "TRIP_SURVEY": TRIP_SURVEY,
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
