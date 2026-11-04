"""
Main script driver that delete PMD data from Arcadis on the S3 bucket its4us-performance-metrics-data (legacy bucket).

"""

import os
import boto3
from pytz import timezone
from datetime import datetime, timedelta


class DeletePMDArcadis:

    def __init__(self, PARAMS: dict) -> None:

        self.input_date = PARAMS["INPUT_DATE"] # e.g. 20240402, a specific date to delete the data for the date
        self.s3_bucket = PARAMS["ARCADIS_PMD_BUCKET"]
        self.s3_folder = PARAMS["ARCADIS_PMD_FOLDER"]
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
        # today_date = self.now.strftime(self.date_format_no_dash) # 20240402
        # self.weekday_num = self.now.weekday() # 0 to 6 for Monday to Sunday

        if not self.input_date:

            message = "WARNING: No input date is specified by user, set the date to yesterday's date" \
                + " where Arcadis data are available for deletion"
            print(message)

            # set the date input to today's date or yesterday's date?
            #self.input_date = today_date

            yesterday_date = self.now - timedelta(days = 1)
            self.input_date = yesterday_date.strftime(self.date_format_no_dash) # 20240401

        # construct the input date with YYYY-MM-DD format
        date_formatted = datetime.strptime(self.input_date, self.date_format_no_dash)
        date_formatted = self.eastern.localize(date_formatted)
        self.input_date_formatted = date_formatted.strftime(self.date_format_with_dash) # 2024-04-01

        # determine the Monday and Sunday dates where the input date is located
        self.dates = self.find_date_range()

        self.monday_date = self.dates[0] # a starting Monday date
        self.sunday_date = self.dates[-1] # an end Sunday date

        # construct the folder path of Arcadis data located on S3
        self.pmd_s3_folder_name = "PMD_" + self.monday_date + "_" + self.sunday_date # e.g. PMD_2024-11-25_2024-12-01

    
    def find_date_range(self) -> list[str]:

        print("Finding the date range of PMD specified by the input date:", self.input_date)

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
    

    def delete_data_arcadis(self) -> None:

        files_to_delete = []

        # find the PMD data from Arcadis on S3 for deletion on the specified input date
        for pmd_name in self.pmd_names:

                # construct the full file path on S3 for the file deletion
                pmd_filename = self.input_date_formatted + "-" + pmd_name + "." + self.zip_extension # e.g. 2024-11-25-MonitoredTrip.zip
                pmd_filepath = os.path.join(self.s3_folder, self.pmd_s3_folder_name, pmd_filename)

                current_file = {"Key": pmd_filepath}

                files_to_delete.append(current_file)

        # construct the full file path to delete the PMD survey data on S3
        survey_filename = self.input_date_formatted + "-" + self.survey_zipfile # e.g. 2024-11-25-merged.zip
        survey_filepath = os.path.join(self.s3_folder, self.pmd_s3_folder_name, survey_filename)
        current_file = {"Key": survey_filepath}

        files_to_delete.append(current_file)

        # delete the Arcadis data files on S3
        # delete_objects does not error out if files do not exist for deletion
        response = self.s3_client.delete_objects(
            Bucket = self.s3_bucket,
            Delete = {
                "Objects": files_to_delete,
                "Quiet": False
            }
        )

        print(response)

        message = "Done deleting the data files from Arcadis "
        message += f"on date {self.input_date_formatted} from S3 bucket: {self.s3_bucket}"

        print(message)

        return
    
