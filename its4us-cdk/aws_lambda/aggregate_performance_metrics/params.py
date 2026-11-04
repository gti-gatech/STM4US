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
        S3_PERFORMANCE_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"] # internal upload bucket
        S3_PERFORMANCE_FOLDER = input_params["stageVariables"]["PERFORMANCE_METRICS_FOLDER"] # e.g. performance-metrics
        S3_PUBLIC_EXPORT_BUCKET = input_params["stageVariables"]["PUBLIC_EXPORT_BUCKET"]

        query_params = input_params.get("queryStringParameters", {}) or {}

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")
        S3_PERFORMANCE_BUCKET = os.environ.get("UPLOAD_BUCKET") # internal upload bucket
        S3_PERFORMANCE_FOLDER = os.environ.get("PERFORMANCE_METRICS_FOLDER")

        # export aggregated PMD metrics to PUBLIC_EXPORT_BUCKET
        S3_PUBLIC_EXPORT_BUCKET = os.environ.get("PUBLIC_EXPORT_BUCKET")

        query_params = event.get("queryStringParameters", {}) or {}
    
    # set the input week to empty string if not specified
    WEEK = query_params.get("week", "")
    if WEEK:
        print("The optional week input is specified and is", WEEK)
    else:
        print("The optional week input is not specified")

    # set the metrics output filename to record aggregated PMD metrics
    S3_WEEKLY_OUTPUT_METRICS_FILE = "Weekly_PMD.json" # fixed output filename for ICF

    TRIP_METRICS_NAMES = {
        "date": "date",
        "deviatedLinks": "deviatedLinks",
        "timestampsDeviated": "timestampsDeviated",
        "completed": "completed",
        "trips completed count": "trips completed count",
        "abandoned": "abandoned",
        "trips abandoned count": "trips abandoned count",
        "numDeviations": "numDeviations",
        "trips deviated count": "trips deviated count",
        "numIntersectionDeviations": "numIntersectionDeviations",
        "trips deviated at intersection count": "trips deviated at intersection count",
        "fixedTransit": "fixedTransit",
        "fixed transit used count": "fixed transit used count",
        "unique users count": "unique users count",
        "trips requested count": "trips requested count",
    }

    PMD_METRICS_NAMES = {
        "date": "date",
        "CT_ME_1_12_total_trips_requested": "CT_ME_1_12_total_trips_requested",
        "CT_ME_1_11_GMAP_registered_users": "CT_ME_1_11_GMAP_registered_users",
        "CT_ME_1_12_total_trips_completed": "CT_ME_1_12_total_trips_completed",
        "CT_ME_1_12_total_trips_abandoned": "CT_ME_1_12_total_trips_abandoned",
        "CT_ME_1_8_total_trips_deviated": "CT_ME_1_8_total_trips_deviated",
        "CT_ME_3_2_total_trips_deviated_at_intersection": "CT_ME_3_2_total_trips_deviated_at_intersection",
        "CT_ME_4_2_total_fixed_transit_used": "CT_ME_4_2_total_fixed_transit_used",
    }

    DATE_FORMAT_NO_DASH = "%Y%m%d"
    DATE_FORMAT_WITH_DASH = "%Y-%m-%d"
    BODY = "Body"
    PMD = "PMD"
    JSON_EXTENSION = "json"
    TIMEZONE = "US/Eastern"

    SUCCESS_MESSAGE = "SUCCESS: WHOLE PROCESS IS COMPLETED FOR THE REQUEST" 

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
        "S3_PERFORMANCE_BUCKET": S3_PERFORMANCE_BUCKET,
        "S3_PERFORMANCE_FOLDER": S3_PERFORMANCE_FOLDER,
        "S3_PUBLIC_EXPORT_BUCKET": S3_PUBLIC_EXPORT_BUCKET,
        "WEEK": WEEK,
        "S3_WEEKLY_OUTPUT_METRICS_FILE": S3_WEEKLY_OUTPUT_METRICS_FILE,
        "TRIP_METRICS_NAMES": TRIP_METRICS_NAMES,
        "PMD_METRICS_NAMES": PMD_METRICS_NAMES,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "DATE_FORMAT_NO_DASH": DATE_FORMAT_NO_DASH,
        "DATE_FORMAT_WITH_DASH": DATE_FORMAT_WITH_DASH,
        "BODY": BODY,
        "PMD": PMD,
        "JSON_EXTENSION": JSON_EXTENSION,
        "TIMEZONE": TIMEZONE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS