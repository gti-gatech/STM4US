"""
The script consists of functions to compute basic performance metrics of STM such as:
    * Unique users count on Sunday (last day of the date range)
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

import traceback
from typing import Any

from trip_evaluation import calculate_deviations

class ComputePMDMetrics:

    def __init__(self, PARAMS: dict[str, Any], pmd_data: dict[str, list[dict]]) -> None:

        self.params = PARAMS
        self.pmd_names = PARAMS["PMD_NAMES"]
        self.trip_metrics_names = PARAMS["TRIP_METRICS_NAMES"]
        self.monitored_trip_id = PARAMS["MONITORED_TRIP_ID"]
        self.tracked_journey_id = PARAMS["TRACKED_JOURNEY_ID"]
        self.pmd_data = pmd_data # a dictionary

    
    def compute_trips_requested_count(self) -> int:

        # compute number of trips requested during a day
        trip_requests = self.pmd_data[self.pmd_names["trips requested"]]
        count = len(trip_requests)

        return count
    

    def compute_unique_users_count(self) -> int:

        # compute number of unique users during a day
        system_users = self.pmd_data[self.pmd_names["unique users"]]
        count = len(system_users)

        return count
    

    def compute_trips_completed_deviated_count(self) -> tuple[dict[str, int], bool]:

        # compute number of trips completed, abandoned and deviated during a day
        trip_monitored = self.pmd_data[self.pmd_names["monitored trips"]]
        tracked_journeys = self.pmd_data[self.pmd_names["tracked journeys"]]

        num_trips_completed = 0
        num_trips_abandoned = 0
        num_trips_deviated = 0
        num_trips_deviated_at_intersection = 0
        num_fixed_transit_used = 0

        # check tracked journey trips to make sure all trips are accounted for
        check_tracked_journey = False
        num_tracked_journeys = len(tracked_journeys)
        track_num_matched_trips = 0

        for mtrip in trip_monitored:
            
            try:
                tracked_journey = next(trip for trip in tracked_journeys if trip[self.tracked_journey_id] == mtrip[self.monitored_trip_id])
            
            except Exception as warning:

                traceback_string = traceback.format_exc()

                # display a warning message
                warning_message = "WARNING: Unable to find the associated tracked journey " \
                    + "for the trip monitored id: " + mtrip[self.monitored_trip_id] \
                        + ". The associated metrics: trips completed count, trips abandoned count, " \
                            + "trips deviated count, may not be accurate from the run."
                
                print(warning_message)
                print(warning)
                print(f"TRACEBACK:\n{traceback_string}")

                continue # continue to the next trip monitored

            track_num_matched_trips += 1
            
            # compute metrics for the monitored trip and tracked journey found
            metric = calculate_deviations(self.params, mtrip, tracked_journey)

            # store the metrics computed
            if metric[self.trip_metrics_names["completed"]] == True:
                num_trips_completed += 1
            else:
                num_trips_abandoned += 1

            num_trips_deviated += metric[self.trip_metrics_names["numDeviations"]]
            num_trips_deviated_at_intersection += metric[self.trip_metrics_names["numIntersectionDeviations"]]

            if metric[self.trip_metrics_names["fixedTransit"]] is True:
                num_fixed_transit_used += 1

        # check to ensure every tracked journey is accounted for
        if track_num_matched_trips != num_tracked_journeys:
            message = "WARNING: Not every tracked journey trip is accounted for in the PMD calculation: "
            message += f"total tracked journey trips is {num_tracked_journeys} "
            message += f"while only {track_num_matched_trips} of the trips is accounted for"

            check_tracked_journey = True

            print(message)

        # return the overall metrics for the day
        output = {self.trip_metrics_names["trips completed count"]: num_trips_completed, 
                  self.trip_metrics_names["trips abandoned count"]: num_trips_abandoned,
                  self.trip_metrics_names["trips deviated count"]: num_trips_deviated, 
                  self.trip_metrics_names["trips deviated at intersection count"]: num_trips_deviated_at_intersection,
                  self.trip_metrics_names["fixed transit used count"]: num_fixed_transit_used}
        
        return output, check_tracked_journey

            