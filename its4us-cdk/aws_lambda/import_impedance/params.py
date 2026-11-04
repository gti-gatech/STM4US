import os

def find_params(event):

    PARAMS = dict()

    query_params = event.get("queryStringParameters", {}) or {}
    FILENAME = str(query_params.get("filename", ""))
    LOADER_URL = os.environ.get("LOADER_URL")
    QUERY_URL = os.environ.get("QUERY_URL")

    print("FILENAME:", FILENAME)

    # Import impedance data from UPLOAD_BUCKET
    UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")

    # set the search query Lambda function to run based on the env
    SEARCH_IMPEDANCE_LAMBDA = os.environ.get("SEARCH_QUERY_IMPEDANCE_ARN", "")

    ORIG_FILEPATH = "/" + UPLOAD_BUCKET + "/" + FILENAME
    NEW_FILEPATH = "impedance" + "/" + FILENAME

    BULKLOAD_JSON = {
        "source" : "s3://{}/impedance".format(UPLOAD_BUCKET),
        "format" : "csv",
        "iamRoleArn" : os.environ.get("NEPTUNE_S3_ROLE_ARN"),
        "region" : os.environ.get("AWS_REGION_NAME"),
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE",
        "dependencies" : []
    }

    PARAMS = {
        "FILENAME": FILENAME,
        "LOADER_URL": LOADER_URL,
        "QUERY_URL": QUERY_URL,
        "UPLOAD_BUCKET": UPLOAD_BUCKET,
        "SEARCH_IMPEDANCE_LAMBDA": SEARCH_IMPEDANCE_LAMBDA,
        "ORIG_FILEPATH": ORIG_FILEPATH,
        "NEW_FILEPATH": NEW_FILEPATH,
        "BULKLOAD_JSON": BULKLOAD_JSON,
    }

    return PARAMS