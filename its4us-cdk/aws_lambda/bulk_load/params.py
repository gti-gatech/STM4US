import os
import json

def find_params(event):
    """Extract parameters from environment variables and event."""
    
    # Environment variables (from CDK)
    LOADER_URL = os.environ.get("LOADER_URL")
    UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET")
    
    # Extract from event body
    if isinstance(event.get("body"), str):
        body_dict = json.loads(event["body"])
    else:
        body_dict = event.get("body", {})
    
    location = body_dict.get("location")
    
    PARAMS = {
        "LOADER_URL": LOADER_URL,
        "UPLOAD_BUCKET": UPLOAD_BUCKET,
        "location": location,
    }
    
    return PARAMS
