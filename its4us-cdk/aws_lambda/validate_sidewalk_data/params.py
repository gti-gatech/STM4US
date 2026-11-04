import os
import datetime

def find_params(event):
    """Extract parameters from environment variables and event."""
    
    # Environment variables (from CDK)
    UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")
    LOG_BUCKET = os.environ.get("ADMIN_LOG_BUCKET")
    
    # Extract from query string parameters
    query_params = event.get("queryStringParameters", {}) or {}
    location = query_params.get("location")

    FACTORS_FILE_KEY = os.environ.get("FACTORS_FILE_KEY", "factors.csv")
    LAMBDA_FACTORS_FILE = "/tmp/factors.csv"

    VALIDATION_LOG_FILE = f"validation_log_{datetime.datetime.now()}.csv"
    VALIDATION_HEADERS = ["File", "Row", "Column", "Info"]

    FACTOR_FIELDS = {
        "Variable Name": "Variable Name",
        "Units": "Units",
        "Enumerated": "Enumerated",
    }
    
    PARAMS = {
        "UPLOAD_BUCKET": UPLOAD_BUCKET,
        "LOG_BUCKET": LOG_BUCKET,
        "location": location,
        "FACTORS_FILE_KEY": FACTORS_FILE_KEY,
        "LAMBDA_FACTORS_FILE": LAMBDA_FACTORS_FILE,
        "VALIDATION_LOG_FILE": VALIDATION_LOG_FILE,
        "VALIDATION_HEADERS": VALIDATION_HEADERS,
        "FACTOR_FIELDS": FACTOR_FIELDS,
    }
    
    return PARAMS
