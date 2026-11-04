import numpy as np
import pandas as pd
import datetime
import boto3
import os
from pathlib import Path
import csv
from params import find_params

def lambda_handler(event, context):
    params = find_params(event)
    
    location = params["location"]
    UPLOAD_BUCKET = params["UPLOAD_BUCKET"]
    LOG_BUCKET = params["LOG_BUCKET"]
    FACTORS_FILE_KEY = params["FACTORS_FILE_KEY"]
    LAMBDA_FACTORS_FILE = params["LAMBDA_FACTORS_FILE"]
    VALIDATION_LOG_FILE = params["VALIDATION_LOG_FILE"]
    VALIDATION_HEADERS = params["VALIDATION_HEADERS"]
    FACTOR_FIELDS = params["FACTOR_FIELDS"]

    chars_to_remove = " :"
    # Create a mapping table where each character maps to None
    table = str.maketrans("", "", chars_to_remove)
    validation_log_filename = VALIDATION_LOG_FILE.translate(table)

    validation_log_file = f"/tmp/{validation_log_filename}"
    with open(validation_log_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(VALIDATION_HEADERS)

    s3 = boto3.client("s3")
    s3.download_file(UPLOAD_BUCKET, FACTORS_FILE_KEY, LAMBDA_FACTORS_FILE)
    factors = pd.read_csv(LAMBDA_FACTORS_FILE, na_values="NA")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=UPLOAD_BUCKET, Prefix=location)

    num_errors_total = 0

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                s3_key = obj["Key"]
                # Create the corresponding local path
                # Ensures correct path separators for different OS
                local_file_path = os.path.join("/tmp", os.path.relpath(s3_key, location))
                
                # Create parent directories if they don"t exist
                Path(os.path.dirname(local_file_path)).mkdir(parents=True, exist_ok=True)
                
                # Download the file
                # Skip if the "object" is actually a folder key (ends with "/")
                if not s3_key.endswith("/"):
                    print(f"Downloading {s3_key} to {local_file_path}")
                    s3.download_file(UPLOAD_BUCKET, s3_key, local_file_path)
                    num_errors = validate_data(local_file_path, factors, validation_log_file, 
                                               FACTOR_FIELDS)
                    num_errors_total = num_errors_total + num_errors
    
    if num_errors_total > 0:
        s3.upload_file(validation_log_file, LOG_BUCKET, validation_log_filename)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", 
                        "Access-Control-Allow-Headers": "Content-Type", 
                        "Access-Control-Allow-Origin":"*", 
                        "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
            "body": f"Uploaded log to {validation_log_filename}"
        }
    else:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json",
                        "Access-Control-Allow-Headers": "Content-Type",
                        "Access-Control-Allow-Origin":"*",
                        "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE"},
            "body": "No errors found"
        }

def validate_data(local_file_path, factors, validation_log_file, FACTOR_FIELDS):
    num_errors = 0

    data = pd.read_csv(local_file_path)

    present_factors = factors[factors[FACTOR_FIELDS["Variable Name"]].isin(data.columns)]
    nonenumerated_factors = present_factors[present_factors[FACTOR_FIELDS["Units"]] != FACTOR_FIELDS["Enumerated"]]

    nonenumerated_data = data[nonenumerated_factors[FACTOR_FIELDS["Variable Name"]]]

    non_float_mask = nonenumerated_data.map(lambda x: not isinstance(x, (float, np.float64, np.float32)))

    with open(validation_log_file, "a", newline="") as f:
        writer = csv.writer(f)
        
        rows, cols = np.where(non_float_mask)
        for row, col in zip(rows, cols):
            csvrow = [local_file_path, row, nonenumerated_data.columns[col], "Non-float found in enumerated column"]

            writer.writerow(csvrow)
            num_errors = num_errors + 1
    
    return num_errors