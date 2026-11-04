"""
Main script driver that retrieve PMD data from Arcadis using the URLs they have provided.

"""

import os
import boto3
import requests
from pytz import timezone
from datetime import datetime, timedelta


class RetrievePMDArcadis:

    def __init__(self, PARAMS: dict) -> None:

        self.input_date = PARAMS["INPUT_DATE"] # e.g. 20240402, a specific date to download the data for the date
        self.url_root = PARAMS["URL_ROOT"]
        self.headers = {"x-api-key": PARAMS["ARCADIS_API_KEY"]}
        self.s3_bucket = PARAMS["S3_UPLOAD_BUCKET"] 
        self.s3_folder = PARAMS["S3_UPLOAD_FOLDER"]
        self.trip_survey = PARAMS["TRIP_SURVEY"]
        self.pmd_names = PARAMS["PMD_NAMES"]
        self.date_format_no_dash = PARAMS["DATE_FORMAT_NO_DASH"]
        self.date_format_with_dash = PARAMS["DATE_FORMAT_WITH_DASH"]
        self.zip_extension = PARAMS["ZIP_EXTENSION"]
        self.timezone = PARAMS["TIMEZONE"]
        self.survey_zipfile = PARAMS["SURVEY_ZIPFILE"]

        # initialize s3 client
        self.s3_client = boto3.client("s3")

        # store current time in EST/EDT
        self.eastern = timezone(self.timezone)
        self.now = datetime.now(self.eastern) # datetime object
        today_date = self.now.strftime(self.date_format_no_dash) # 20240402
        # self.weekday_num = self.now.weekday() # 0 to 6 for Monday to Sunday

        # check if the date specified by user is in the future or is today's date
        if self.input_date:

            input_date = datetime.strptime(self.input_date, self.date_format_no_dash)
            input_date_obj = self.eastern.localize(input_date) # datetime object

            if input_date_obj > self.now or self.input_date == today_date:

                # user specified date is in the future or today's date, reset the date to yesterday's date
                message = "WARNING: User specified date parameter is in the future or today's date. " \
                    + "Setting the date to yesterday's date for retrieving the full dataset."
                print(message)

                # the input date is in the future or today's date, reset it to the yesterday's date
                yesterday_date = self.now - timedelta(days = 1)
                self.input_date = yesterday_date.strftime(self.date_format_no_dash) # 20240401
        
        else:

            message = "WARNING: No input date is specified by user, set the date to yesterday's date" \
                + " where Arcadis data are available for retrieval"
            print(message)

            # set the input date to yesterday's date
            yesterday_date = self.now - timedelta(days = 1)
            self.input_date = yesterday_date.strftime(self.date_format_no_dash) # 20240401
            
        # construct the input date with YYYY-MM-DD format
        date_formatted = datetime.strptime(self.input_date, self.date_format_no_dash)
        date_formatted = self.eastern.localize(date_formatted)
        self.input_date_formatted = date_formatted.strftime(self.date_format_with_dash) # 2024-04-02

        # determine the Monday and Sunday dates where the input date is located
        self.dates = self.find_date_range()

        self.monday_date = self.dates[0] # a starting Monday date
        self.sunday_date = self.dates[-1] # an end Sunday date

        # construct the path of URLs to retrieve data from Arcadis
        self.pmd_dates_range = "PMD_" + self.monday_date + "_" + self.sunday_date # e.g. PMD_2024-11-25_2024-12-01
        self.pmd_url = os.path.join(self.url_root, self.pmd_dates_range)
        self.survey_url = os.path.join(self.url_root, self.trip_survey)

    
    def find_date_range(self) -> list[str]:

        print("Finding the date range of PMD specified by the user input date:", self.input_date)

        input_date = datetime.strptime(self.input_date, self.date_format_no_dash)
        input_date = self.eastern.localize(input_date) # timezone aware

        dates = []

        # find the starting Monday of the date range where input date is located
        monday_num = input_date.weekday()
        monday = input_date - timedelta(days = monday_num) 
        date_str = monday.strftime(self.date_format_with_dash) # e.g. 2024-04-01

        # save the Monday date found
        dates.append(date_str)

        # find the ending Sunday of the date range where input date is located
        sunday_num = monday + timedelta(days = 6)
        date_str = sunday_num.strftime(self.date_format_with_dash) # e.g. 2024-04-01

        # save the Sunday date found
        dates.append(date_str)

        print("MONDAY AND SUNDAY DATE RANGE OF PMD:", dates)

        return dates
    

    def retrieve_upload_data_arcadis(self) -> None:

        # retrieve PMD data from Arcadis URLs for each data type and upload to S3 for the specified input date
        for pmd_name in self.pmd_names:

                # construct the full URL to retrieve the PMD data
                pmd_filename = self.input_date_formatted + "-" + pmd_name + "." + self.zip_extension # e.g. 2024-11-25-MonitoredTrip.zip
                pmd_url = os.path.join(self.pmd_url, pmd_filename)
                
                # use the URL to retrieve PMD data and save to /tmp/ folder
                res = requests.get(pmd_url, headers = self.headers)
                local_filepath = os.path.join("/tmp", pmd_filename) # e.g. /tmp/2024-11-25-MonitoredTrip.zip

                with open(local_filepath, mode="wb") as file:
                    file.write(res.content)

                # upload the downloaded file to S3
                s3_filename = os.path.join(self.s3_folder, self.pmd_dates_range, pmd_filename) # e.g. <s3_folder>/PMD_2024-11-25_2024-12-01/2024-11-25-MonitoredTrip.zip
                self.s3_client.upload_file(local_filepath, self.s3_bucket, s3_filename)

        # construct the full URL to retrieve the PMD survey data
        survey_filename = self.input_date_formatted + "-" + self.survey_zipfile # e.g. 2024-11-25-merged.zip
        survey_url = os.path.join(self.survey_url, survey_filename)

        # use the URL to retrieve PMD survey data and save to /tmp/ folder
        res = requests.get(survey_url, headers = self.headers)
        local_filepath = os.path.join("/tmp", survey_filename) # e.g. /tmp/2024-11-25-merged.zip

        with open(local_filepath, mode="wb") as file:
            file.write(res.content)

        # upload the downloaded file to S3
        s3_filename = os.path.join(self.s3_folder, self.pmd_dates_range, survey_filename) # e.g. PMD_2024-11-25_2024-12-01/2024-11-25-merged.zip
        self.s3_client.upload_file(local_filepath, self.s3_bucket, s3_filename)

        return
    
