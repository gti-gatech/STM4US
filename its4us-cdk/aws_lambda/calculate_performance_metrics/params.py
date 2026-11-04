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

        if input_params["stageVariables"]["ENV"] == "prod":
            # use Arcadis data files on the legacy bucket for the calculation
            S3_ARCADIS_PMD_BUCKET = input_params["stageVariables"]["ARCADIS_PMD_BUCKET"] # legacy bucket
            S3_ARCADIS_PMD_FOLDER = ""
        else:
            # use Arcadis data files on the upload bucket and folder for the calculation
            S3_ARCADIS_PMD_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"] # internal upload bucket
            S3_ARCADIS_PMD_FOLDER = input_params["stageVariables"]["PERFORMANCE_METRICS_FOLDER"] # e.g. performance-metrics

        # use upload bucket and folder for storing the daily computed PMD
        S3_UPLOAD_BUCKET = input_params["stageVariables"]["UPLOAD_BUCKET"] # internal upload bucket
        S3_UPLOAD_FOLDER = input_params["stageVariables"]["PERFORMANCE_METRICS_FOLDER"] # e.g. performance-metrics

        query_params = input_params.get("queryStringParameters", {}) or {}

    else:

        JOB_ID = ""        # set CodePipeline job id and name to empty
        FUNCTION_NAME = ""
        PIPELINE_NAME = ""

        QUERY_URL = os.environ.get("QUERY_URL")
        ADMIN_LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")

        if os.environ.get("STAGE") == "prod":
            # use Arcadis data files on the legacy bucket for the calculation
            S3_ARCADIS_PMD_BUCKET = os.environ.get("ARCADIS_PMD_BUCKET") # legacy bucket
            S3_ARCADIS_PMD_FOLDER = ""
        else:
            # use Arcadis data files on the upload bucket and folder for the calculation
            S3_ARCADIS_PMD_BUCKET = os.environ.get("UPLOAD_BUCKET") # internal upload bucket
            S3_ARCADIS_PMD_FOLDER = os.environ.get("PERFORMANCE_METRICS_FOLDER") # e.g. performance-metrics

        # use upload bucket and folder for storing the daily computed PMD
        S3_UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET") # internal upload bucket
        S3_UPLOAD_FOLDER = os.environ.get("PERFORMANCE_METRICS_FOLDER") # e.g. performance-metrics

        query_params = event.get("queryStringParameters", {}) or {}
    
    # set the input date to empty string if not specified
    INPUT_DATE = query_params.get("date", "")
    if INPUT_DATE:
        print("The optional date input is specified and is", INPUT_DATE)
    else:
        print("The optional date input is not specified")

    PMD_NAMES = {
        "TripRequest": "TripRequest",
        "trips requested": "trips requested",
        "OtpUser": "OtpUser",
        "unique users": "unique users",
        "MonitoredTrip": "MonitoredTrip",
        "monitored trips": "monitored trips",
        "TrackedJourney": "TrackedJourney",
        "tracked journeys": "tracked journeys",
    }
   
    TRIP_EVAL_FIELDS = {
        "itinerary": "itinerary",
        "legs": "legs",
        "locations": "locations",
        "mode": "mode",
        "steps": "steps",
        "streetName": "streetName",
        "route": "route",
    }

    TRIP_MODE_WALK = "WALK"
    TRIP_MODE_BUS = "BUS"

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
        "CT_ME_1_8_average_trips_deviated": "CT_ME_1_8_average_trips_deviated",
        "CT_ME_3_2_average_trips_deviated_at_intersection": "CT_ME_3_2_average_trips_deviated_at_intersection",
        "CT_ME_4_2_average_fixed_transit_used": "CT_ME_4_2_average_fixed_transit_used",
    }
    
    MONITORED_TRIP_ID = "id"
    TRACKED_JOURNEY_ID = "tripId"

    METER_TO_FEET = 3.28084 # 1 meter is 3.28084 feet
    BUFFER_LENGTH = 20.0 # ft

    STUDY_AREA = {
        "34.0N84.4W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.4, "max_lon": -84.3},
        "33.8N84.4W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.4, "max_lon": -84.3},
        "33.9N84.4W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.4, "max_lon": -84.3},
        "33.8N84.1W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.1, "max_lon": -84.0},
        "34.0N84.3W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.3, "max_lon": -84.2},
        "33.8N84.3W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.3, "max_lon": -84.2},
        "33.9N84.3W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.3, "max_lon": -84.2},
        "33.8N84.2W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.2, "max_lon": -84.1},
        "33.9N84.2W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.2, "max_lon": -84.1},
        "34.0N84.2W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.2, "max_lon": -84.1},
        "33.9N84.1W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.1, "max_lon": -84.0},
        "34.0N84.1W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.1, "max_lon": -84.0},
        "33.9N84.0W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.0, "max_lon": -83.9},
        "34.0N84.0W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.0, "max_lon": -83.9},
    }

    STUDY_AREA_BOUNDARY = {
        "min_lat": "min_lat",
        "max_lat": "max_lat",
        "min_lon": "min_lon",
        "max_lon": "max_lon",
    }

    OSM_CROSSWALK_ATTRS = {
        "footway": "footway",
        "crossing": "crossing",
        "traffic_signals": "traffic_signals",
        "__datasetid": "__datasetid",
    }

    OSM_NODE_LABEL = "OSM-NODE"
    OSM_WAY_LABEL = "OSM-WAY"
    ARBITRARY_STUDY_AREA = "34.0N84.4W"
    DATE_FORMAT_NO_DASH = "%Y%m%d"
    DATE_FORMAT_WITH_DASH = "%Y-%m-%d"
    PMD = "PMD"
    BODY = "Body"
    ZIP_EXTENSION = "zip"
    JSON_EXTENSION = "json"
    GEOSPACE = "WGS84"
    TIMEZONE = "US/Eastern"

    SUCCESS_MESSAGE = "SUCCESS: WHOLE PROCESS IS COMPLETED FOR THE REQUEST"
    WARNING_MESSAGE = "WARNING: WHOLE PROCESS IS COMPLETED WITH WARNING. PLEASE CHECK CLOUDWATCH LOGS"

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
        "QUERY_URL": QUERY_URL,
        "ADMIN_LOG_BUCKET": ADMIN_LOG_BUCKET,
        "S3_ARCADIS_PMD_BUCKET": S3_ARCADIS_PMD_BUCKET,
        "S3_ARCADIS_PMD_FOLDER": S3_ARCADIS_PMD_FOLDER,
        "S3_UPLOAD_BUCKET": S3_UPLOAD_BUCKET,
        "S3_UPLOAD_FOLDER": S3_UPLOAD_FOLDER,
        "INPUT_DATE": INPUT_DATE,
        "PMD_NAMES": PMD_NAMES,
        "TRIP_EVAL_FIELDS": TRIP_EVAL_FIELDS,
        "TRIP_MODE_WALK": TRIP_MODE_WALK,
        "TRIP_MODE_BUS": TRIP_MODE_BUS,
        "TRIP_METRICS_NAMES": TRIP_METRICS_NAMES,
        "PMD_METRICS_NAMES": PMD_METRICS_NAMES,
        "MONITORED_TRIP_ID": MONITORED_TRIP_ID,
        "TRACKED_JOURNEY_ID": TRACKED_JOURNEY_ID,
        "METER_TO_FEET": METER_TO_FEET,
        "BUFFER_LENGTH": BUFFER_LENGTH,
        "STUDY_AREA": STUDY_AREA,
        "STUDY_AREA_BOUNDARY": STUDY_AREA_BOUNDARY,
        "OSM_CROSSWALK_ATTRS": OSM_CROSSWALK_ATTRS,
        "OSM_NODE_LABEL": OSM_NODE_LABEL,
        "OSM_WAY_LABEL": OSM_WAY_LABEL,
        "ARBITRARY_STUDY_AREA": ARBITRARY_STUDY_AREA,
        "DATE_FORMAT_NO_DASH": DATE_FORMAT_NO_DASH,
        "DATE_FORMAT_WITH_DASH": DATE_FORMAT_WITH_DASH,
        "PMD": PMD,
        "BODY": BODY,
        "ZIP_EXTENSION": ZIP_EXTENSION,
        "JSON_EXTENSION": JSON_EXTENSION,
        "GEOSPACE": GEOSPACE,
        "TIMEZONE": TIMEZONE,
        "SUCCESS_MESSAGE": SUCCESS_MESSAGE,
        "WARNING_MESSAGE": WARNING_MESSAGE,
        "STATUS": STATUS,
        "CODEPIPELINE_STATUS": CODEPIPELINE_STATUS,
    }

    return PARAMS