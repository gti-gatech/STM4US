"""
The script consists of main functions to run and compute basic performance metrics of STM such as:
    * Unique users count on Sunday (last day of the date range)
    * Trips requested count
    * Completed trips count
    * Abandoned trips count
    * Trips deviated count
    * Trips deviated at intersections count
    * Fixed transit used count

The resulting metrics are saved to a JSON file and uploaded to a S3 bucket.

UPDATED on 08/21/26: PMD metrics are calculated daily and are saved to S3 bucket for aggregation later.
Users could specify a date for the calculation, or it is defaulted to current day.
The daily PMD metrics are saved to the internal S3 upload bucket in a folder called "performance-metrics"
with filename "PMD_<YYYY-MM-DD>_<YYYY-MM-DD>.json".
If PMD metrics were calculated for the date already, it will get updated in the daily output file only if
the Arcadis PMD data are found, otherwise the PMD metrics for the date are not updated in the daily output file.
    
"""

import os
import boto3
import json
import traceback
from pytz import timezone
from datetime import datetime, timedelta
from typing import Any

from retrieve_pmd_data_s3 import RetrievePMDDataS3
from compute_performance_metrics import ComputePMDMetrics


class PMDMetricsRunDaily:

    def __init__(self, PARAMS: dict[str, Any]) -> None:

        self.params = PARAMS
        self.input_date = PARAMS["INPUT_DATE"] # str
        self.s3_upload_bucket = PARAMS["S3_UPLOAD_BUCKET"] # internal upload bucket
        self.s3_upload_folder = PARAMS["S3_UPLOAD_FOLDER"] # e.g. performance-metrics
        self.pmd_names = PARAMS["PMD_NAMES"]
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

        if self.input_date:

            # convert the input date to dashed format (string)
            self.input_date = datetime.strptime(self.input_date, self.date_format_no_dash)
            self.input_date = self.input_date.strftime(self.date_format_with_dash)

        else:

            # set the input date to the current day (string)
            #self.input_date = self.now.strftime(self.date_format_with_dash)

            message = "WARNING: No input date is specified by user, set the date to yesterday's date" \
                + " where Arcadis data are available for PMD calculation"
            print(message)

            # set the input date to yesterday's date
            yesterday_date = self.now - timedelta(days = 1)
            self.input_date = yesterday_date.strftime(self.date_format_with_dash) # 2024-04-01

        # determine the week-long date range based on the input date
        self.dates = self.find_date_range()
            
        self.monday_date = self.dates[0] # a starting Monday date
        self.sunday_date = self.dates[-1] # an end Sunday date

        pmd_dates_range = self.params["PMD"] + "_" + self.monday_date + "_" + self.sunday_date

        # set the daily metrics output filename; e.g. performance-metrics/PMD_<YYYY-MM-DD>_<YYYY-MM-DD>.json
        s3_output_metrics_daily_file = pmd_dates_range + "." + self.json_extension
        self.s3_output_metrics_outfile = os.path.join(self.s3_upload_folder, s3_output_metrics_daily_file)

        # retrieve existing metrics output located on S3
        self.retrieve_metrics_s3()

        # initialize RetrievePMDDataS3 class object to retrieve PMD data for a single day
        s3_subdirectory = pmd_dates_range + "/" # e.g. PMD_<YYYY-MM-DD>_<YYYY-MM-DD>/
        self.retrievePMDObj = RetrievePMDDataS3(self.params, s3_subdirectory, self.input_date)

    
    def find_date_range(self) -> list[str]:

        # convert input date to a datetime object
        input_date = datetime.strptime(self.input_date, self.date_format_with_dash)
        input_date = self.eastern.localize(input_date) # timezone aware

        dates = []
        
        # determine the date range based on the input date starting on a Monday
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
    

    def read_pmd_files(self, date: str) -> tuple[dict[str, list[dict[str, Any]]], bool]:

        data_exist = False

        # read PMD data files in zip for each data type on the date
        trip_requests, requests_exist = self.retrievePMDObj.read_pmd_file(self.pmd_names["TripRequest"], date) # a list of dictionaries
        system_users, users_exist = self.retrievePMDObj.read_pmd_file(self.pmd_names["OtpUser"], date) # a list of dictionaries
        trip_monitored, monitored_exist = self.retrievePMDObj.read_pmd_file(self.pmd_names["MonitoredTrip"], date) # a list of dictionaries
        tracked_journeys, journey_exist = self.retrievePMDObj.read_pmd_file(self.pmd_names["TrackedJourney"], date) # a list of dictionaries

        # store the imported data in a dictionary
        pmd_data = {self.pmd_names["trips requested"]: trip_requests, 
                    self.pmd_names["unique users"]: system_users,
                    self.pmd_names["monitored trips"]: trip_monitored, 
                    self.pmd_names["tracked journeys"]: tracked_journeys}

        if requests_exist and users_exist and monitored_exist and journey_exist:
            data_exist = True

        return pmd_data, data_exist
    

    def compute_pmd_metrics(self, pmd_data: dict[str, list[dict]], date: str) -> tuple[dict[str, Any], bool]:

        # compute the metrics based on the PMD data for the day
        computePMDMetricsObj = ComputePMDMetrics(self.params, pmd_data)
        unique_users_count = computePMDMetricsObj.compute_unique_users_count()
        trips_requested_count = computePMDMetricsObj.compute_trips_requested_count()
        trips_completed_deviated_count, check_tracked_journey = computePMDMetricsObj.compute_trips_completed_deviated_count()

        # save the computed metrics
        metrics = {self.trip_metrics_names["date"]: date, 
                   self.trip_metrics_names["unique users count"]: unique_users_count, 
                   self.trip_metrics_names["trips requested count"]: trips_requested_count}
        
        metrics.update(trips_completed_deviated_count)

        return metrics, check_tracked_journey
    

    def retrieve_metrics_s3(self) -> None:

        # load existing metrics from the metric output file on S3

        try:
            response = self.s3_client.get_object(Bucket = self.s3_upload_bucket, 
                                                 Key = self.s3_output_metrics_outfile)
            metrics_output_str = response[self.body].read().decode() 
            self.metrics_output = json.loads(metrics_output_str) # a list of dictionaries

        except Exception as warning:

            traceback_string = traceback.format_exc()

            warning_message = "WARNING: PMD daily output file: " + self.s3_output_metrics_outfile \
                + " cannot be found in the S3 bucket " + self.s3_upload_bucket \
                    + ". The file will be created in the S3 bucket"
            print(warning_message)
            print(warning)
            print(f"TRACEBACK:\n{traceback_string}")

            # specify an empty metrics_output as input
            self.metrics_output = []

        return


    def upload_metrics_s3(self) -> None:

        # save the new computed metrics to json file and upload it to S3
        data = json.dumps(self.metrics_output)
        
        self.s3_client.put_object(Body = data, Bucket = self.s3_upload_bucket, 
                                  Key = self.s3_output_metrics_outfile)

        message = "SUCCESS: Computed PMD is successfully uploaded to S3: " + self.s3_upload_bucket \
            + " in the file: " + self.s3_output_metrics_outfile
        print(message)

        return

    
    def run_compute_pmd_metrics(self) -> bool:

        # load PMD data files in zip for each data type of a single day
        pmd_data, data_exist = self.read_pmd_files(self.input_date)

        # compute the metrics based on the PMD data loaded
        metrics, check_tracked_journey = self.compute_pmd_metrics(pmd_data, self.input_date)

        # save the computed metrics to computed_metrics to compute aggregated metrics
        #computed_metrics.append(metrics)

        # compute aggregated metrics
        #dates_range = self.monday_date + "_" + self.sunday_date # e.g. "2024-04-01_2024-04-07"
        #aggregated_metrics = self.compute_aggregated_pmd_metrics(computed_metrics, dates_range)
        
        # find the aggregated metrics in the existing metrics output to update
        metric_found = next((metric for metric in self.metrics_output if metric[self.trip_metrics_names["date"]] == self.input_date), False)

        if metric_found:

            message = f"WARNING: PMD metrics for the date on {self.input_date} were found in the output file: " + \
                f"{self.s3_output_metrics_outfile}"
            print(message)

            if data_exist:
                # update the metrics with the latest computed values only if the data exist
                metric_found.update(metrics) # self.metrics_output is also updated

                message = f"WARNING: PMD metrics for the date on {self.input_date} were updated from the current run " + \
                    f"in the output file: {self.s3_output_metrics_outfile}"

            else:

                # show message that the metrics in the output file are not updated from the current run
                message = f"WARNING: Arcadis data were not found. PMD metrics for the date on {self.input_date} " + \
                    f"were NOT updated from the current run in the output file: {self.s3_output_metrics_outfile}"

        else:

            # append the computed metrics to the metrics output
            self.metrics_output.append(metrics)

            message = f"PMD metrics for the date on {self.input_date} were added in the output file: " + \
                f"{self.s3_output_metrics_outfile}"

        print(message)
        # save the new computed metrics to json and upload to S3
        self.upload_metrics_s3()

        return check_tracked_journey