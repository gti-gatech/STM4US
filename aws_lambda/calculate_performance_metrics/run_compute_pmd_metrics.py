"""
The script consists of main functions to run and compute basic performance metrics of STM such as:
    * Unique users count
    * Trips requested count
    * Completed trips count
    * Abandoned trips count
    * Trips deviated count
    * Trips deviated at intersections count
    * Fixed transit used count
    * Aggregated or average counts of above metrics for past 7 days from the current day

The resulting metrics are saved to a JSON file and uploaded to a S3 bucket.

UPDATED on 08/23/24: Currently only the aggregated metrics are saved and uploaded to the S3 bucket
inside a json file named Weekly_PMD.json, and individual metric on each day is not saved.
    
"""


import boto3
import json
from pytz import timezone
from datetime import datetime, timedelta
from typing import Any

from retrieve_pmd_data_s3 import RetrievePMDDataS3
from compute_performance_metrics import ComputePMDMetrics


class PMDMetricsRun:

    def __init__(self, env: str, week: str, s3_bucket: str, query_url: str, public_bucket: str) -> None:

        self.env = env # dev, prod
        self.week = week # e.g. 20240401
        self.s3_bucket_output = public_bucket # a public bucket
        self.query_url = query_url
        # initialize s3 client
        self.s3_client = boto3.client("s3")

        # store current time in EST/EDT
        self.eastern = timezone("US/Eastern")
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

        # construct the metrics output filename to retrieve and upload metrics
        # e.g. PMD_<YYYY-MM-DD>_<YYYY-MM-DD>.json; PMD_2024-05-06_2024-05-12.json
        #pmd_dates_range = "PMD_" + self.monday_date + "_" + self.sunday_date
        #self.s3_metric_file_internal = pmd_dates_range + ".json"
        self.s3_metric_file = "Weekly_PMD.json" # fixed output filename for ICF

        # retrieve existing metrics output located on S3
        self.retrieve_metrics_s3()

        # initialize RetrievePMDDataS3 class object to retrieve PMD data
        pmd_dates_range = "PMD_" + self.monday_date + "_" + self.sunday_date
        s3_subdirectory = pmd_dates_range + "/" # e.g. PMD_<YYYY-MM-DD>_<YYYY-MM-DD>/
        self.retrievePMDObj = RetrievePMDDataS3(s3_bucket, s3_subdirectory, self.dates)

    
    def find_date_range_by_user(self) -> list[str]:

        # check if the date specified by user is in the future
        input_date = datetime.strptime(self.week, "%Y%m%d")
        input_date = self.eastern.localize(input_date) # timezone aware

        if input_date > self.now:

            # user specified date is in the future, compute metrics based on last week's data
            message = "WARNING: User specified week parameter is in the future. " \
                + "PMD metrics will be computed using last week's data located on S3"
            print(message)

            dates = self.find_date_range()

        else:

            print("Constructing date range of PMD specified by the user input week:", self.week)

            dates = []

            # determine the date range based on the user input starting on a Monday
            monday_num = input_date.weekday()
            monday = input_date - timedelta(days = monday_num) 
            date_str = monday.strftime("%Y-%m-%d") # e.g. 2024-04-01

            # save the Monday date found
            dates.append(date_str)

            for day in range(1, 7):

                datetime_check = monday + timedelta(days = day)
                date_str = datetime_check.strftime("%Y-%m-%d") # e.g. 2024-04-01

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
            date_str = datetime_check.strftime("%Y-%m-%d") # e.g. 2024-04-01

            # save the date found
            dates.append(date_str)

        print("DATE RANGE OF PMD:", dates)

        return dates
    

    def read_pmd_files(self, date: str) -> dict[str, list[dict[str, Any]]]:

        # read PMD data files in zip for each data type on the date
        trip_requests = self.retrievePMDObj.read_pmd_file("TripRequest", date) # a list of dictionaries
        system_users = self.retrievePMDObj.read_pmd_file("Persona", date) # a list of dictionaries
        trip_monitored = self.retrievePMDObj.read_pmd_file("MonitoredTrip", date) # a list of dictionaries
        tracked_journeys = self.retrievePMDObj.read_pmd_file("TrackedJourney", date) # a list of dictionaries

        # store the imported data in a dictionary
        pmd_data = {"trips requested": trip_requests, "unique users": system_users,
                    "monitored trips": trip_monitored, "tracked journeys": tracked_journeys}

        return pmd_data
    

    def compute_pmd_metrics(self, pmd_data: dict[str, list[dict]], date: str) -> dict[str, Any]:

        # compute the metrics based on the PMD data for the day
        computePMDMetricsObj = ComputePMDMetrics(self.env, pmd_data, self.query_url)
        unique_users_count = computePMDMetricsObj.compute_unique_users_count()
        trips_requested_count = computePMDMetricsObj.compute_trips_requested_count()
        trips_completed_deviated_count = computePMDMetricsObj.compute_trips_completed_deviated_count()

        # save the computed metrics
        metrics = {"date": date, "unique users count": unique_users_count, 
                   "trips requested count": trips_requested_count}
        
        metrics.update(trips_completed_deviated_count)

        return metrics
    

    def compute_aggregated_pmd_metrics(self, computed_metrics: list[dict[str, Any]], 
                                       dates_range: str) -> dict[str, Any]:

        # compute aggregated PMD metrics from the 7-day period
        total_trips_requested = 0
        total_unique_users = 0
        total_trips_completed = 0
        total_trips_abandoned = 0
        total_trips_deviated = 0
        total_trips_deviated_at_intersection = 0
        total_fixed_transit_used = 0

        for metrics in computed_metrics:

            # compute the total trips requested 
            total_trips_requested += metrics["trips requested count"]

            # compute the total unique users in the system
            total_unique_users += metrics["unique users count"]

            # compute the total trips completed
            total_trips_completed += metrics["trips completed count"]

            # compute the total trips abandoned
            total_trips_abandoned += metrics["trips abandoned count"]

            # compute the total trips deviated
            total_trips_deviated += metrics["trips deviated count"]

            # compute the total trips deviated at traffic light intersection
            total_trips_deviated_at_intersection += metrics["trips deviated at intersection count"]

            # compute the total fixed transit used
            total_fixed_transit_used += metrics["fixed transit used count"]

        # compute average of unique users
        average_unique_users = total_unique_users / 7.0

        # compute average of trips deviated
        average_trips_deviated = total_trips_deviated / 7.0

        # compute the average of trips deviated at traffic light intersection
        average_trips_deviated_at_intersection = total_trips_deviated_at_intersection / 7.0

        # compute the average of fixed transit routes used
        average_fixed_transit_used = total_fixed_transit_used / 7.0

        # store the computed metrics
        aggregated_metrics = {"date": dates_range, "total trips requested": total_trips_requested,
                              "average unique users": average_unique_users, 
                              "total trips completed": total_trips_completed,
                              "total trips abandoned": total_trips_abandoned,
                              "average trips deviated": average_trips_deviated,
                              "average trips deviated at intersection": average_trips_deviated_at_intersection,
                              "average fixed transit used": average_fixed_transit_used}

        return aggregated_metrics
    

    def retrieve_metrics_s3(self) -> None:

        # load existing metrics from the metric output file on S3
        try:

            response = self.s3_client.get_object(Bucket = self.s3_bucket_output, Key = self.s3_metric_file)
            metrics_output_str = response["Body"].read().decode() 
            self.metrics_output = json.loads(metrics_output_str) # a list of dictionaries

        except:

            message = "WARNING: PMD output file: " + self.s3_metric_file + " cannot be found in the S3 bucket " \
                + self.s3_bucket_output + ". The file will be created in the S3 bucket"
            print(message)

            # specify an empty metrics_output as input
            self.metrics_output = []

        return


    def upload_metrics_s3(self) -> None:

        # save the new computed metrics to json file and upload it to S3
        data = json.dumps(self.metrics_output)
        
        self.s3_client.put_object(Body = data, Bucket = self.s3_bucket_output, Key = self.s3_metric_file)

        return

    
    def run_compute_pmd_metrics(self) -> None:

        # load or compute the metrics for each day
        computed_metrics = []
        new_metrics_to_save = [] # save new computed metrics only to json

        # compute PMD metrics for all of the date range
        for date in self.dates:

            # load PMD data files in zip for each data type
            pmd_data = self.read_pmd_files(date)

            # compute the metrics based on the PMD data loaded
            metrics = self.compute_pmd_metrics(pmd_data, date)

            # save the computed metrics to computed_metrics to compute aggregated metrics
            computed_metrics.append(metrics)

            # UPDATED: individual metric for each day is not saved to json files
            # find the metrics in the existing metrics output to update
            #metric_found = next((metric for metric in self.metrics_output if metric["date"] == date), False)
            #
            #if metric_found:
            #
            #    # update the metrics with the latest computed values
            #    metric_found.update(metrics) # self.metrics_output is also updated
            #
            #else:
            #
            #    # save the new computed metrics as a reference to save to json file
            #    new_metrics_to_save.append(metrics)

        # compute aggregated metrics
        dates_range = self.monday_date + "_" + self.sunday_date # e.g. "2024-04-01_2024-04-07"
        aggregated_metrics = self.compute_aggregated_pmd_metrics(computed_metrics, dates_range)
        
        # find the aggregated metrics in the existing metrics output to update
        metric_found = next((metric for metric in self.metrics_output if metric["date"] == dates_range), False)

        if metric_found:

            # update the aggregated metrics with the latest computed values
            metric_found.update(aggregated_metrics) # self.metrics_output is also updated

        else:

            # save the new computed aggregated metrics as a reference to save to json file
            new_metrics_to_save.append(aggregated_metrics)

        # save the computed metrics for the new dates to the overall self.metrics_output
        self.metrics_output += new_metrics_to_save

        # save the new computed metrics to json and upload to S3; only aggregated metrics are saved
        self.upload_metrics_s3()

        return