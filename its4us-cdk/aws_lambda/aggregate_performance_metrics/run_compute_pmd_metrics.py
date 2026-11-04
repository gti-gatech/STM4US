"""
The script consists of main functions to run and compute aggregated performance metrics of STM such as:
    * Unique users count on Sunday (last day of the date range)
    * Total trips requested count
    * Total completed trips count
    * Total abandoned trips count
    * Total trips deviated count
    * Total trips deviated at intersections count
    * Total fixed transit used count

The resulting metrics are saved to a JSON file and uploaded to a S3 bucket.

UPDATED on 08/21/26: The aggregated (total) PMD results are computed by loading the the json file from
the internal S3 upload bucket in a folder called "performance-metrics" with filename 
"PMD_<YYYY-MM-DD>_<YYYY-MM-DD>.json" containing the computed PMD metrics daily. The function simply
adds the metrics stored from the file together and saves to the external S3 public bucket.

"""


import boto3
import json
import traceback
from pytz import timezone
from datetime import datetime, timedelta
from typing import Any


class PMDMetricsRunWeekly:

    def __init__(self, PARAMS: dict[str, Any]) -> None:

        self.params = PARAMS
        self.week = PARAMS["WEEK"] # str
        self.s3_upload_bucket = PARAMS["S3_PERFORMANCE_BUCKET"] # internal upload bucket
        self.s3_upload_folder = PARAMS["S3_PERFORMANCE_FOLDER"] # e.g. performance-metrics
        self.s3_public_export_bucket = PARAMS["S3_PUBLIC_EXPORT_BUCKET"]
        self.s3_weekly_output_metrics_file = PARAMS["S3_WEEKLY_OUTPUT_METRICS_FILE"]
        self.trip_metrics_names = PARAMS["TRIP_METRICS_NAMES"]
        self.pmd_metrics_names = PARAMS["PMD_METRICS_NAMES"]
        self.date_format_no_dash = PARAMS["DATE_FORMAT_NO_DASH"]
        self.date_format_with_dash = PARAMS["DATE_FORMAT_WITH_DASH"]
        self.body = PARAMS["BODY"]
        self.json_extension = PARAMS["JSON_EXTENSION"]
       
        # initialize s3 client
        self.s3_client = boto3.client("s3")

        # store current time in EST/EDT
        self.eastern = timezone(self.params["TIMEZONE"])
        self.now = datetime.now(self.eastern)
        self.weekday_num = self.now.weekday() # 0 to 6 for Monday to Sunday

        if self.week:

            # determine the date range based on user input
            self.dates = self.find_date_range_by_user()
        
        else:

            # find dates of last week's 7 days of the current day
            self.dates = self.find_date_range()


        self.monday_date = self.dates[0] # a starting Monday date
        self.sunday_date = self.dates[-1] # an end Sunday date

        self.dates_range = self.monday_date + "_" + self.sunday_date

        # retrieve existing metrics output located on S3 (aggregated)
        self.retrieve_aggregated_metrics_s3()


    def find_date_range_by_user(self) -> list[str]:

        # check if the date specified by user is in the future
        input_date = datetime.strptime(self.week, self.date_format_no_dash)
        input_date = self.eastern.localize(input_date) # timezone aware

        if input_date > self.now:

            # user specified date is in the future, compute metrics based on last week's data
            message = "WARNING: User specified week parameter is in the future. " \
                + "PMD metrics will be computed using last week's data located on S3"
            print(message)

            dates = self.find_date_range()

        else:

            print("Constructing date range of PMD specified by the user input week:", 
                  self.week)

            dates = []

            # determine the date range based on the user input starting on a Monday
            monday_num = input_date.weekday()
            monday = input_date - timedelta(days = monday_num) 
            date_str = monday.strftime(self.date_format_with_dash) # e.g. 2024-04-01

            # save the Monday date found
            dates.append(date_str)

            for day in range(1, 7):

                datetime_check = monday + timedelta(days = day)
                date_str = datetime_check.strftime(self.date_format_with_dash) # e.g. 2024-04-01

                # save the date found
                dates.append(date_str)

        print("DATE RANGE OF PMD:", dates)

        return dates
    
    
    def find_date_range(self) -> list[str]:

        # find the Monday to Sunday days of last week based on the current day
        last_monday_num = self.weekday_num + 7 # always run last week's data starting on Monday
        dates = []

        # find the last week days from Monday to Sunday
        for day in range(0, 7):

            datetime_check = self.now - timedelta(days = last_monday_num - day) 
            date_str = datetime_check.strftime(self.date_format_with_dash) # e.g. 2024-04-01

            # save the date found
            dates.append(date_str)

        print("DATE RANGE OF PMD:", dates)

        return dates
    

    def compute_aggregated_pmd_metrics(self) -> dict[str, Any]:

        # compute aggregated PMD metrics from the 7-day period
        total_trips_requested = 0
        #total_unique_users = 0
        total_trips_completed = 0
        total_trips_abandoned = 0
        total_trips_deviated = 0
        total_trips_deviated_at_intersection = 0
        total_fixed_transit_used = 0

        for metrics in self.daily_metrics_output:

            # compute the total trips requested 
            total_trips_requested += metrics[self.trip_metrics_names["trips requested count"]]

            # compute the total unique users in the system
            #total_unique_users += metrics["unique users count"]

            # compute the total trips completed
            total_trips_completed += metrics[self.trip_metrics_names["trips completed count"]]

            # compute the total trips abandoned
            total_trips_abandoned += metrics[self.trip_metrics_names["trips abandoned count"]]

            # compute the total trips deviated
            total_trips_deviated += metrics[self.trip_metrics_names["trips deviated count"]]

            # compute the total trips deviated at traffic light intersection
            total_trips_deviated_at_intersection += metrics[self.trip_metrics_names["trips deviated at intersection count"]]

            # compute the total fixed transit used
            total_fixed_transit_used += metrics[self.trip_metrics_names["fixed transit used count"]]

        # retrieve the user count on Sunday only
        sunday_data = next((metric for metric in self.daily_metrics_output if metric.get("date") == self.sunday_date), {})
        sunday_users = sunday_data.get("unique users count") or 0

        # store the computed metrics
        aggregated_metrics = {self.pmd_metrics_names["date"]: self.dates_range, 
                              self.pmd_metrics_names["CT_ME_1_12_total_trips_requested"]: total_trips_requested,
                              self.pmd_metrics_names["CT_ME_1_11_GMAP_registered_users"]: sunday_users,
                              self.pmd_metrics_names["CT_ME_1_12_total_trips_completed"]: total_trips_completed,
                              self.pmd_metrics_names["CT_ME_1_12_total_trips_abandoned"]: total_trips_abandoned,
                              self.pmd_metrics_names["CT_ME_1_8_total_trips_deviated"]: total_trips_deviated,
                              self.pmd_metrics_names["CT_ME_3_2_total_trips_deviated_at_intersection"]: total_trips_deviated_at_intersection,
                              self.pmd_metrics_names["CT_ME_4_2_total_fixed_transit_used"]: total_fixed_transit_used}
        
        return aggregated_metrics
    

    def retrieve_aggregated_metrics_s3(self) -> None:

        # load existing metrics from the metric output file on S3 (aggregated)
        try:

            response = self.s3_client.get_object(Bucket = self.s3_public_export_bucket, 
                                                 Key = self.s3_weekly_output_metrics_file)
            metrics_output_str = response[self.body].read().decode() 
            self.weekly_metrics_output = json.loads(metrics_output_str) # a list of dictionaries

        except Exception as warning:

            traceback_string = traceback.format_exc()

            warning_message = "WARNING: PMD weekly output file: " + self.s3_weekly_output_metrics_file \
                + " cannot be found in the S3 bucket " + self.s3_public_export_bucket \
                    + ". The file will be created in the S3 bucket"
            print(warning_message)
            print(warning)
            print(f"TRACEBACK:\n{traceback_string}")

            # specify an empty metrics_output as input
            self.weekly_metrics_output = []

        return


    def retrieve_daily_metrics_s3(self) -> None:

        # load existing daily metrics from the metric output file on S3 (individual)
        try:

            response = self.s3_client.get_object(Bucket = self.s3_upload_bucket, 
                                                 Key = self.s3_daily_output_metrics_file)
            metrics_output_str = response[self.body].read().decode() 
            self.daily_metrics_output = json.loads(metrics_output_str) # a list of dictionaries

        except Exception as warning:

            traceback_string = traceback.format_exc()

            warning_message = "WARNING: PMD daily output file: " + self.s3_daily_output_metrics_file \
                + " cannot be found in the S3 bucket " + self.s3_upload_bucket \
                    + ". Please run daily PMD calculations first."
            print(warning_message)
            print(warning)
            print(f"TRACEBACK:\n{traceback_string}")

            self.daily_metrics_output = []

        return


    def upload_aggregated_metrics_s3(self) -> None:

        # save the new computed metrics to json file and upload it to S3
        data = json.dumps(self.weekly_metrics_output)
        
        self.s3_client.put_object(Body = data, Bucket = self.s3_public_export_bucket, 
                                  Key = self.s3_weekly_output_metrics_file)

        message = "SUCCESS: Computed weekly PMD is successfully uploaded to S3: " \
            + self.s3_public_export_bucket + " in the file: " + self.s3_weekly_output_metrics_file
        print(message)

        return

    
    def run_compute_pmd_metrics(self) -> None:

        # retrieve existing daily metrics output located on S3 (individual); e.g. performance-metrics/PMD_<YYYY-MM-DD>_<YYYY-MM-DD>.json
        pmd_dates_range = self.params["PMD"] + "_" + self.monday_date + "_" + self.sunday_date
        s3_output_metrics_daily_file = pmd_dates_range + "." + self.json_extension
        self.s3_daily_output_metrics_file = self.s3_upload_folder + "/" + s3_output_metrics_daily_file
        self.retrieve_daily_metrics_s3()

        if len(self.daily_metrics_output) == 7: # each file should have 7 days of data

            new_metrics_to_save = [] # save new computed metrics only to json

            # continue to compute the PMD metric totals
            aggregated_metrics = self.compute_aggregated_pmd_metrics()

            # find the aggregated metrics in the existing metrics output to update
            metric_found = next((metric for metric in self.weekly_metrics_output if metric[self.trip_metrics_names["date"]] == self.dates_range), False)

            if metric_found:

                message = f"WARNING: PMD metrics for the week: {self.dates_range} was found in the PMD weekly output file: " + \
                    f"{self.s3_weekly_output_metrics_file} in the S3 bucket: {self.s3_public_export_bucket}. "
                message += "The PMD data in the file will be updated from the current run."
                print(message)

                # update the aggregated metrics with the latest computed values
                metric_found.update(aggregated_metrics) # self.weekly_metrics_output is also updated

            else:

                # save the new computed aggregated metrics
                new_metrics_to_save.append(aggregated_metrics)

            # save the computed metrics for the week to the overall self.weekly_metrics_output
            self.weekly_metrics_output += new_metrics_to_save

            # save the aggregated metrics to json and upload to S3
            self.upload_aggregated_metrics_s3()

        else:

            warning_message = "WARNING: PMD daily output file: " + self.s3_daily_output_metrics_file \
                + " was not found in the S3 bucket " + self.s3_upload_bucket \
                    + " or does not contain whole 7-days of data. Please check and run daily" \
                        + " PMD calculations for each day of the week first. "
            warning_message += f"The file {self.s3_public_export_bucket}/{self.s3_weekly_output_metrics_file} " \
                + "is not updated."
                            
            print(warning_message)


        return