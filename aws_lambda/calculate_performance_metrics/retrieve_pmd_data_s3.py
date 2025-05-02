"""
The file retrieves PMD data in zip files on AWS S3 bucket for a range of 7 days specified with
the input dates_range.

The zip files have filenames such as 2024-02-16-TripRequest.zip and contain only one JSON file 
inside the zip such as 2024-02-16-TripRequest.json, assuming the JSON file has the same filename
as the zip file.

"""


import io
import boto3
import json
from typing import Any
import zipfile


class RetrievePMDDataS3:

    def __init__(self, s3_bucket: str, s3_subdirectory: str = "", dates_range: list[str] = []) -> None:

        self.s3_resource = boto3.resource("s3")

        self.bucket = s3_bucket # NOTE: temporary bucket
        self.folder = s3_subdirectory # e.g. PMD_2024-04-01_2024-04-07/
        self.dates = dates_range


    def retrieve_file_name(self, file_type: str = "TripRequest", date_str: str = "") -> str:

        # construct or build a zip file name to read the file specified by the inputs
        file_name = self.folder + date_str + "-" + file_type + ".zip" # e.g. <self.folder>2024-04-01-TripRequest.zip

        return file_name


    def retrieve_file_names(self, file_type: str = "TripRequest") -> list[str]:

        # construct or build zip file names to read the files for previous 7 days
        file_names = []

        for date in self.dates:

            file_name = self.folder + date + "-" + file_type + ".zip" # e.g. <self.folder>2024-04-01-TripRequest.zip

            # save the filename constructed
            file_names.append(file_name)

        return file_names
    

    def read_pmd_file(self, file_type: str = "TripRequest", date_str: str = "") -> list[dict[str, Any]]:

        pmd_data = []

        # retrieve the zip file name to read for a single day/file
        file_name = self.retrieve_file_name(file_type, date_str)

        try:

            # read the PMD data zip file specified by the inputs
            zip_obj = self.s3_resource.Object(bucket_name = self.bucket, 
                                              key = file_name)
            
            buffer = io.BytesIO(zip_obj.get()["Body"].read())
            zfp = zipfile.ZipFile(buffer, "r")

            # read the file inside the zip, NOTE: assume same file name as zip in json
            zip_file = file_name.split("/")[1] # 2024-02-16-TripRequest.zip
            json_file = zip_file[:-3] + "json" # e.g. 2024-02-16-TripRequest.json
        
            with zfp.open(json_file) as data_file:

                data_read = data_file.read()
                json_data = json.loads(data_read)

                # save the read data
                pmd_data = json_data # a list

        except Exception as error:

            # display a warning message
            warning = "WARNING: The PMD file failed to be loaded from S3: " + file_name \
                + ", the associated " + file_type + " metrics may not be accurate from the run."
            print(warning)
            print(error)

        return pmd_data


    def read_pmd_files(self, file_type: str = "TripRequest") -> list[list[dict[str, Any]]]:

        # retrieve file names on reading the zip files for the last 7 days
        file_names = self.retrieve_file_names(file_type)

        # read PMD data files in zip on S3 bucket for the last 7 days
        pmd_data = [] # a list of lists

        for file_name in file_names:
            
            try:

                zip_obj = self.s3_resource.Object(bucket_name = self.bucket, 
                                                  key = file_name)
                buffer = io.BytesIO(zip_obj.get()["Body"].read())
                zfp = zipfile.ZipFile(buffer, "r")

                # read the file inside the zip, NOTE: assume same file name as zip in json
                zip_file = file_name.split("/")[1] # 2024-02-16-TripRequest.zip
                json_file = zip_file[:-3] + "json" # e.g. 2024-02-16-TripRequest.json

                with zfp.open(json_file) as data_file:

                    data_read = data_file.read()
                    json_data = json.loads(data_read)

                    # save the read data
                    pmd_data.append(json_data)
            
            except Exception as error:

                # display a warning message
                warning = "WARNING: The PMD file failed to be loaded from S3: " + json_file \
                + ", the associated " + file_type + " metrics may not be accurate from the run."
                print(warning)
                print(error)

                continue # continue to load next json file

        return pmd_data
    

    