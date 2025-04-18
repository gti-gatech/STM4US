import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo


class PreprocessNavigatorData:

    def __init__(self, scheduled_events, unscheduled_events, comments, properties):

        self.scheduled_events = scheduled_events
        self.unscheduled_events = unscheduled_events
        self.comments = comments
        self.properties = properties

        self.eastern = ZoneInfo("US/Eastern") # handles EST/EDT conversion


    def add_modified_date_ms(self, row):

        # add a new field storing modified date in millisecond
        modified_date_str = row["modified_date"]

        #modified_date_ms = datetime.strptime(modified_date_str, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000
        modified_date_ms = datetime.strptime(modified_date_str, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo = self.eastern).timestamp() * 1000

        return modified_date_ms


    def preprocess_scheduled_events(self):

        # preprocess or clean up raw scheduled events data
        data = [] # list of dictionaries
        headers = []

        lines = self.scheduled_events.split("\r\n")
        for line in lines:

            fields = line.split(",")

            # extract headers
            if fields[0] == "event_id":
                headers = fields # test if the header row is found

            try:
                id1 = int(fields[0]) # test if an actual data row is found
            except:
                continue
            
            # actual data row found, store the data in the row
            count = 0
            row = {}
            for header in headers:

                row[header] = fields[count]

                if header == "modified_date":

                    # add a new field storing modified date in millisecond for future nodes/links processing
                    modified_date_ms = self.add_modified_date_ms(row)
                    row["__modified_date_ms"] = modified_date_ms
                
                count += 1

            data.append(row)

        # convert data to dataframe
        df = pd.DataFrame.from_dict(data)

        # default all fields to strings first
        df = df.convert_dtypes()

        if not df.empty:
            # convert version and severity fields to int64, latitude, longitude and __modified_date_ms to float64
            df = df.astype({"version": "int", "severity": "int", "latitude": "float", "longitude": "float", 
                            "__modified_date_ms": "float"})

        return df
    

    def preprocess_unscheduled_events(self):

        # preprocess or clean up raw unscheduled events data
        data = [] # list of dictionaries
        headers = []

        lines = self.unscheduled_events.split("\r\n")
        for line in lines:

            fields = line.split(",")

            # extract headers
            if fields[0] == "event_id":
                headers = fields # test if the header row is found

            try:
                id1 = int(fields[0]) # test if an actual data row is found
            except:
                continue
            
            # actual data row found, store the data in the row
            count = 0
            row = {}
            for header in headers:

                row[header] = fields[count]

                if header == "modified_date":

                    # add a new field storing modified date in millisecond for future nodes/links processing
                    modified_date_ms = self.add_modified_date_ms(row)
                    row["__modified_date_ms"] = modified_date_ms
                
                count += 1

            data.append(row)

        # convert data to dataframe
        df = pd.DataFrame.from_dict(data)

        # default all fields to strings first
        df = df.convert_dtypes()

        if not df.empty:
            # convert version and severity fields to int64, latitude, longitude and __modified_date_ms to float64
            df = df.astype({"version": "int", "severity": "int", "latitude": "float", "longitude": "float", 
                            "__modified_date_ms": "float"})

        return df
    

    def preprocess_comments(self):

        # preprocess or clean up raw comments data
        data = [] # list of dictionaries
        headers = []
        num_headers = 0

        lines = self.comments.split("\r\n")
        for line in lines:

            fields = line.split("|")

            # extract headers
            if fields[0] == "comment_id":
                headers = fields # test if the header row is found
                num_headers = len(headers)

            try:
                id1 = int(fields[0]) # test if an actual data row is found
            except:
                continue

            # actual data row found, store the data in the row
            num_fields = len(fields)

            if num_fields == num_headers: # skip comments with missing fields

                count = 0
                row = {}
                for header in headers:

                    row[header] = fields[count]
                    count += 1

                data.append(row)

        # convert data to dataframe
        df = pd.DataFrame.from_dict(data)

        # default all fields to strings
        df = df.convert_dtypes()

        return df
    

    def preprocess_properties(self):

        # preprocess or clean up raw property data
        data = [] # list of dictionaries
        headers = []
        num_headers = 0

        lines = self.properties.split("\r\n")
        for line in lines:

            fields = line.split("|")

            # extract headers
            if fields[0] == "property_id":
                headers = fields # test if the header row is found
                num_headers = len(headers)

            try:
                id1 = int(fields[0]) # test if an actual data row is found
            except:
                continue

            # actual data row found, store the data in the row
            num_fields = len(fields)

            if num_fields == num_headers: # skip properties with missing fields

                count = 0
                row = {}
                for header in headers:

                    row[header] = fields[count]
                    count += 1

                data.append(row)

        # convert data to dataframe
        df = pd.DataFrame.from_dict(data)

        # default all fields to strings first
        df = df.convert_dtypes()

        if not df.empty:
            # convert version field to int64
            df = df.astype({"version": "int"})

        return df
