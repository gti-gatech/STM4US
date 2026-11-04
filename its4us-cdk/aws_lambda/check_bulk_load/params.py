import os

def find_params(event):
    """Extract parameters from environment variables and event."""
    
    # Environment variables (from CDK)
    BASE_DB_URL = os.environ.get("BASE_DB_URL")
    
    # Extract from query string parameters
    query_params = event.get("queryStringParameters", {}) or {}
    load_id = query_params.get("id")
    page_number = int(query_params.get("pageNumber", 1))
    page_size = int(query_params.get("pageSize", 100))
    
    PARAMS = {
        "BASE_DB_URL": BASE_DB_URL,
        "id": load_id,
        "pageNumber": page_number,
        "pageSize": page_size,
    }
    
    return PARAMS
