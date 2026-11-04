import boto3
import traceback
from pytz import timezone
from datetime import datetime, timedelta


class RetrieveNavigatorDataS3:

    def __init__(self, PARAMS):

        self.s3_client = boto3.client("s3")
        self.get_last_modified = lambda obj: int(obj["LastModified"].strftime("%s"))
        eastern = timezone(PARAMS["TIMEZONE"])
        datetime_check = datetime.now(eastern) - timedelta(hours=1) # subtract 1 hr to handle possible missing files after the current hour
        self.datetime_str = datetime_check.strftime(PARAMS["DATETIME_FORMAT_NO_DASH"])

        self.bucket = PARAMS["NAVIGATOR_BUCKET"]

        self.scheduled_prefix = PARAMS["SCHEDULED_PREFIX"]
        self.unscheduled_prefix = PARAMS["UNSCHEDULED_PREFIX"]
        self.comment_prefix = PARAMS["COMMENT_PREFIX"]
        self.property_prefix = PARAMS["PROPERTY_PREFIX"]

        self.csv_extension = PARAMS["CSV_EXTENSION"]


    def retrieve_scheduled_events(self):

        # retrieve latest modified scheduled events data file on S3
        scheduled_files = self.scheduled_prefix + self.datetime_str + "." + self.csv_extension

        try:
            scheduled_objs = self.s3_client.list_objects_v2(Bucket = self.bucket, 
                                                            Prefix = self.scheduled_prefix, 
                                                            StartAfter = scheduled_files)["Contents"]
            scheduled_file = [obj["Key"] for obj in sorted(scheduled_objs, key = self.get_last_modified)][-1]

        except:
            error_string = traceback.format_exc()
            print(f"TRACEBACK:\n{error_string}")
    
            return None

        # read the file
        scheduled_response = self.s3_client.get_object(Bucket = self.bucket, Key = scheduled_file)
        scheduled_events = scheduled_response["Body"].read().decode()

        print("NaviGAtor scheduled event file retrieved and read:", scheduled_file)

        return scheduled_events
    
    
    def retrieve_unscheduled_events(self):

        # retrieve latest modified unscheduled events data file on S3
        unscheduled_files = self.unscheduled_prefix + self.datetime_str + "." + self.csv_extension

        try:
            unscheduled_objs = self.s3_client.list_objects_v2(Bucket = self.bucket, 
                                                            Prefix = self.unscheduled_prefix, 
                                                            StartAfter = unscheduled_files)["Contents"]
            unscheduled_file = [obj["Key"] for obj in sorted(unscheduled_objs, key = self.get_last_modified)][-1]
        except:
            error_string = traceback.format_exc()
            print(f"TRACEBACK:\n{error_string}")
            
            return None

        # read the file
        unscheduled_response = self.s3_client.get_object(Bucket = self.bucket, Key = unscheduled_file)
        unscheduled_events = unscheduled_response["Body"].read().decode()

        print("NaviGAtor unscheduled event file retrieved and read:", unscheduled_file)

        return unscheduled_events
    

    def retrieve_comments(self):

        # retrieve latest modified comments data file on S3
        comment_files = self.comment_prefix + self.datetime_str + "." + self.csv_extension

        try:
            comment_objs = self.s3_client.list_objects_v2(Bucket = self.bucket, 
                                                        Prefix = self.comment_prefix, 
                                                        StartAfter = comment_files)["Contents"]
            comment_file = [obj["Key"] for obj in sorted(comment_objs, key = self.get_last_modified)][-1]
        except:
            error_string = traceback.format_exc()
            print(f"TRACEBACK:\n{error_string}")

            return None

        # read the file
        comment_response = self.s3_client.get_object(Bucket = self.bucket, Key = comment_file)
        comments = comment_response["Body"].read().decode()

        print("NaviGAtor event comment file retrieved and read:", comment_file)

        return comments
    

    def retrieve_properties(self):

        # retrieve latest modified properties data file on S3
        property_files = self.property_prefix + self.datetime_str + "." + self.csv_extension

        try:
            property_objs = self.s3_client.list_objects_v2(Bucket = self.bucket, 
                                                        Prefix = self.property_prefix, 
                                                        StartAfter = property_files)["Contents"]
            property_file = [obj["Key"] for obj in sorted(property_objs, key = self.get_last_modified)][-1]
        except:
            error_string = traceback.format_exc()
            print(f"TRACEBACK:\n{error_string}")
            
            return None

        # read the file
        property_response = self.s3_client.get_object(Bucket = self.bucket, Key = property_file)
        properties = property_response["Body"].read().decode()

        print("NaviGAtor event property file retrieved and read:", property_file)

        return properties