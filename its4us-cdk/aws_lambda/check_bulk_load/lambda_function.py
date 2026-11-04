import json
import boto3
from botocore.config import Config
from params import find_params

def lambda_handler(event, context):
    params = find_params(event)
    
    LOAD_URL = params["BASE_DB_URL"]
    load_id = params["id"]
    page_number = params["pageNumber"]
    page_size = params["pageSize"]

    client = boto3.client(
        'neptunedata',
        endpoint_url=LOAD_URL,
        config=Config(read_timeout=None, retries={'total_max_attempts': 1})
    )

    response = client.get_loader_job_status(
        loadId=load_id,
        details=True,
        errors=True,
        errorsPerPage=page_size,
        page=page_number
    )

    return {
            "statusCode": 200,
            "body": json.dumps(response)
        }
