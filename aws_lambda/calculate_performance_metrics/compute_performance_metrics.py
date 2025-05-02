"""
The script consists of functions to compute basic performance metrics of STM such as:
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


from trip_evaluation import calculate_deviations

class ComputePMDMetrics:

    def __init__(self, env: str, pmd_data: dict[str, list[dict]], query_url:str) -> None:

        self.env = env # dev, prod
        self.query_url = query_url
        self.pmd_data = pmd_data # a dictionary

    
    def compute_trips_requested_count(self) -> int:

        # compute number of trips requested during a day
        trip_requests = self.pmd_data["trips requested"]
        count = len(trip_requests)

        return count
    

    def compute_unique_users_count(self) -> int:

        # compute number of unique users during a day
        system_users = self.pmd_data["unique users"]
        count = len(system_users)

        return count
    

    def compute_trips_completed_deviated_count(self) -> dict[str, int]:

        # compute number of trips completed, abandoned and deviated during a day
        trip_requests = self.pmd_data["trips requested"]
        trip_monitored = self.pmd_data["monitored trips"]
        tracked_journeys = self.pmd_data["tracked journeys"]

        num_trips_completed = 0
        num_trips_abandoned = 0
        num_trips_deviated = 0
        num_trips_deviated_at_intersection = 0
        num_fixed_transit_used = 0
        for trip_request in trip_requests:

            try:
                monitored_trip = next(trip for trip in trip_monitored if trip["tripRequestId"] == trip_request["_id"])
                tracked_journey = next(trip for trip in tracked_journeys if trip["tripId"] == monitored_trip["_id"])
            
            except Exception as error:
            
                # display a warning message
                warning = "WARNING: Unable to find the associated monitored trip or the associated tracked journey " \
                    + "for the trip requested id: " + trip_request["_id"] + ". The associated metrics: trips completed " \
                    + "count, trips abandoned count, trips deviated count, may not be accurate from the run."
                
                print(warning)
                print(error)

                continue # continue to the next trip request
            
            # compute metrics for the monitored trip and tracked journey found
            metric = calculate_deviations(self.env, monitored_trip, tracked_journey, self.query_url)

            # store the metrics computed
            if metric["completed"] == True:
                num_trips_completed += 1
            else:
                num_trips_abandoned += 1

            num_trips_deviated += metric["numDeviations"]
            num_trips_deviated_at_intersection += metric["numIntersectionDeviations"]

            if metric["fixedTransit"] is True:
                num_fixed_transit_used += 1

        # return the overall metrics for the day
        output = {"trips completed count": num_trips_completed, "trips abandoned count": num_trips_abandoned,
                  "trips deviated count": num_trips_deviated, 
                  "trips deviated at intersection count": num_trips_deviated_at_intersection,
                  "fixed transit used count": num_fixed_transit_used}
        
        return output

            