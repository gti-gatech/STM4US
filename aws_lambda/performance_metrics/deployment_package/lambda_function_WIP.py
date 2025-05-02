import json
import requests
from pytz import timezone
import datetime
import boto3
from trip_evaluation import calculate_deviations

def lambda_handler(event, context):
    
    today = datetime.date.today()
    dates = [today - datetime.timedelta(i) for i in range(7,0,-1)]
    output = {date: {} for date in dates}

    s3 = boto3.client('s3')
    s3_Bucket_Name = event['stageVariables']['PERFORMANCE_METRICS']
    s3_Dir_Name = dates[0] + '/'
    
    for date in dates:
        tripRequests = json.loads(s3.get_object(Bucket=s3_Bucket_Name, Key=s3_Dir_Name + date + 'tripRequest.csv'))
        monitoredTrips = json.loads(s3.get_object(Bucket=s3_Bucket_Name, Key=s3_Dir_Name + date + 'monitoredTrip.csv'))
        trackedJourneys = json.loads(s3.get_object(Bucket=s3_Bucket_Name, Key=s3_Dir_Name + date + 'trackedJourney.csv'))

        for tripRequest in tripRequests:
            try:
                monitoredTrip = next(trip for trip in monitoredTrips if trip["tripRequestId"] == tripRequest['_id'])
                trackedJourney = next(trip for trip in trackedJourneys if trip["tripId"] == monitoredTrip['_id'])
            except:
                return IndexError("Could not find monitored trip or tracked journey with request id: " + tripRequest['_id'])
            
            result = calculate_deviations(monitoredTrip, trackedJourney)


