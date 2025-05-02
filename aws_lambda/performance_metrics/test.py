from bson import decode_all, json_util
from deployment_package.trip_evaluation import calculate_deviations, dis, deg2rad

f1 = open('./single-monitored-trips-deviated.dat')
f2 = open('./single-tracked-journeys-deviated.dat')
# f2 = open('./single-tracked-journeys-ontrack.dat')
# f2 = open('./single-tracked-journeys-terminated.dat')

# with open('../../pmd/test/single-monitored-trips-deviated.bson','rb') as f:
#     monitoredTrips = eval(f.read())
#     monitoredTrips = json_util.loads(f.read())

# with open('../../pmd/test/single-tracked-journeys-deviated.bson','r') as f:
#     trackedJourneys = json_util.loads(f.read())

monitoredTrips = eval(f1.read())
monitoredTrip = monitoredTrips[0]

trackedJourneys = eval(f2.read())
trackedJourney = trackedJourneys[0]

result = calculate_deviations(monitoredTrip,trackedJourney)

print(result)

# latA = deg2rad(33.940245)
# lonA = deg2rad(-83.984858)
# latB = deg2rad(33.940332)
# lonB = deg2rad(-83.984885)

# distance = dis(latA, lonA, latB, lonB)

# print(distance)