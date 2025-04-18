"""
The script contains varies boto3 functions on S3 bucket and object operations.
Waze alert data on S3 use the function "download_waze_alerts" to download files.

"""

import os
import boto3


class S3BucketObjectOperations:

    def __init__(self, bucket):

        self.bucket = bucket
        self.s3_resource = boto3.resource("s3")
        self.s3_client = boto3.client("s3")

    
    def download_file(self, s3file, outfile):
    
        # download file from S3 to local
        #self.s3_resource.meta.client.download_file(self.bucket, s3file, outfile)

        with open(outfile, "wb") as data:
            self.s3_client.download_fileobj(self.bucket, s3file, data)

        return
    

    def list_objects(self, prefix):

        # get list of objects in a bucket with a prefix
        response = self.s3_client.list_objects_v2(
             Bucket = self.bucket,
             Prefix = prefix, # "10-13-2023/34.0N84.1W"
        )

        return response
    

    def list_objects_start_after(self, prefix, start_after):

        # get list of objects in a bucket with a prefix and with a filename as a starting point of the list
        response = self.s3_client.list_objects_v2(
             Bucket = self.bucket,
             Prefix = prefix, # "<s3_folder>/33.9N84.3W",
             StartAfter = start_after, # "<s3_folder>/33.9N84.3W-2023-10-13-145330.json"
        )

        return response


def create_folder(folder):
        
    # create a folder on local
    try:
        os.makedirs(folder)
        print("Folder created:", folder)
    except:
        print("The folder already exists, skipping creating the folder")

    return


def download_waze_files(S3OperateObj, response, outdir):

    exist = True

    if "Contents" in response.keys():

        count = 0

        # file(s) are found, proceed to download them
        for content in response["Contents"]:
            file = content["Key"] # e.g. <s3_folder>/34.0N84.1W-2023-10-13-145250.json
            filename = file.split("/")[1] # e.g. 34.0N84.1W-2023-10-13-145250.json

            # set up the local filepath to save the file
            outfile = os.path.join(outdir, filename)

            # download the file
            S3OperateObj.download_file(file, outfile)

            count += 1

            if count % 100 == 0:
                print("Number of waze files downloaded:", count)

    else:

        exist = False # there are no more files to download
        file = ""

    return exist, file # "" or last filename in the list


def download_waze_alerts(s3_bucket, s3_folder, outdir, waze_start_date):

    # download waze alerts data files on S3 to local
    study_area = ["34.0N84.4W", "33.8N84.4W", "33.9N84.4W", "33.8N84.1W",
                  "34.0N84.3W", "33.9N84.3W", "33.8N84.2W", "34.0N84.1W", 
                  "33.9N84.1W", "34.0N84.0W", "34.0N84.2W", "33.9N84.2W", 
                  "33.9N84.0W", "33.8N84.3W"]
    
    # download waze alerts data files on S3 for each study area
    for area in study_area:

        print("Working on downloading files for:", area)

        # create subfolder based on outdir and waze_start_date if not exist
        folder = area + "-" + waze_start_date
        outdir1 = os.path.join(outdir, folder)
        create_folder(outdir1)

        prefix = os.path.join(s3_folder, area) # e.g. "<s3_folder>/33.9N84.3W"

        S3OperateObj = S3BucketObjectOperations(s3_bucket)
        response = S3OperateObj.list_objects(prefix)

        # download waze files found on S3
        exist, file = download_waze_files(S3OperateObj, response, outdir1)

        print("Successfully download 1st set of waze alert data files from S3 for:", area)

        while exist:
            
            # find any data files remaining to download
            response1 = S3OperateObj.list_objects_start_after(prefix, file)
            exist, file = download_waze_files(S3OperateObj, response1, outdir1)

            if exist:
                print("Successfully download another set of waze alert data files from S3 for:", area)

        print("Finished downloading waze alert data files from S3 for:", area)
        print("\n")

    print("DONE!")

    return


if __name__ == "__main__":

    # specify inputs
    s3_bucket = '<WAZE_BUCKET>'
    s3_folder = "scheduler"
    outdir = ""
    waze_start_date = "2024-01-16" # use to create subfolders to save waze files on local

    # run the function
    download_waze_alerts(s3_bucket, s3_folder, outdir, waze_start_date)
