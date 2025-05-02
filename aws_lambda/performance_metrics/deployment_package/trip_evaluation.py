from math import sin, cos, asin, acos, atan2, sqrt, pi

MINDIST = 20 # minimum ft away from path that counts as deviation
FINDIST = 100 # maximum ft away from destination that counts as completed trip

def deg2rad(deg):
  return deg * (pi/180)

def dis( latA, lonA, latB, lonB ):
    # DIS Finds the distance between two lat/lon points.
    # Search great-circle distance
    # Must be radians

    R = 20902000
    d = acos( sin(latA)*sin(latB) + cos(latA)*cos(latB)*cos(lonB-lonA) ) * R
    return d

def bear( latA,lonA,latB,lonB ):
    # BEAR Finds the bearing from one lat/lon point to another.
    # Must be radians
    #     
    b = atan2( sin(lonB-lonA)*cos(latB),cos(latA)*sin(latB) - sin(latA)*cos(latB)*cos(lonB-lonA) )
    return b

def distanceFromLineSegment(lat1,lon1,lat2,lon2,lat3,lon3):
    # https://stackoverflow.com/questions/32771458/distance-from-lat-lng-point-to-minor-arc-segment
    # Must be radians

    R = 20902000
    # Prerequisites for the formulas
    bear12 = bear(lat1,lon1,lat2,lon2)
    bear13 = bear(lat1,lon1,lat3,lon3)
    dis13 = dis(lat1,lon1,lat3,lon3)

    diff = abs(bear13-bear12)
    if diff > pi:
        diff = 2 * pi - diff
    # Is relative bearing obtuse?
    if diff>(pi/2):
        dxa=dis13
    else:
        # Find the cross-track distance.
        dxt = asin( sin(dis13/R)* sin(bear13 - bear12) ) * R

        # Is p4 beyond the arc?
        dis12 = dis(lat1,lon1,lat2,lon2)
        dis14 = acos( cos(dis13/R) / cos(dxt/R) ) * R
        if dis14>dis12:
            dxa=dis(lat2,lon2,lat3,lon3)
        else:
            dxa=abs(dxt)
    
    return dxa

def minimum_distance(pointlist, lat, lon):
    min_dist = -1
    for i in range(len(pointlist)-1):
        dist = distanceFromLineSegment(pointlist[i][0], pointlist[i][1], pointlist[i+1][0], pointlist[i+1][1], lat, lon)
        if i == 0 or dist <= min_dist:
            min_dist = dist
            min_i = i
    return (min_dist, min_i)

def calculate_deviations(monitoredTrip, trackedJourney):
    
    legs = monitoredTrip['itinerary']['legs']
    locs = trackedJourney['locations']
    i = 0
    currLoc = locs[i]
    timestampsDeviated = 0
    numDeviations = 0
    onTrack = True
    min_leg_past = 0
    deviatedLinks = []

    for leg in legs:
        legFinished = False
        if leg["mode"] == "WALK":
            min_leg_past = 0
            pointlist = [(deg2rad(step['lat']),deg2rad(step['lon'])) for step in leg['steps']]
            pointlist.append((deg2rad(leg['to']['lat']),deg2rad(leg['to']['lon'])))
            while not legFinished:
                minDist, min_leg = minimum_distance(pointlist, deg2rad(currLoc['lat']), deg2rad(currLoc['lon']))
                if minDist > MINDIST:
                    # deviated
                    if onTrack:
                        # This is the first deviation
                        # Current location/min leg is not accurate because user's already deviated.
                        # Use i-1 and past min leg instead.
                        deviatedLinks.append(leg['steps'][min_leg_past]['streetName'])
                        numDeviations += 1
                        onTrack = False
                        lastTrackLocation = i - 1
                else:
                    if not onTrack:
                        # This is the first time back on track after deviating
                        timestampsDeviated += i - lastTrackLocation - 1
                        onTrack = True
                
                if dis(deg2rad(currLoc['lat']), deg2rad(currLoc['lon']), pointlist[-1][0], pointlist[-1][1]) < MINDIST:
                    legFinished = True
                else:
                    #If not finished, iterate to next point in tracked journey.
                    i += 1
                    if i < len(locs):
                        currLoc = locs[i]
                        
                    else:
                        # Hit the end of the tracked journey without finishing leg.
                        # Still haven't checked rest of the trip after leg.
                        # Deviated completely from leg, but could rejoin on future leg.
                        legFinished = True
                        i = lastTrackLocation + 1
                        currLoc = locs[i]
                min_leg_past = min_leg
                

        else: # Not walking (ie. Bus)
            # Cannot track easily
            # Just check if at end of leg?
            destLat = deg2rad(leg['to']['lat'])
            destLon = deg2rad(leg['to']['lon'])
            if onTrack:
                lastTrackLocation = i - 1
            while not legFinished:
                if dis(deg2rad(currLoc['lat']), deg2rad(currLoc['lon']), destLat, destLon) < MINDIST:
                    legFinished = True
                    if not onTrack:
                        timestampsDeviated += i - lastTrackLocation - 1
                        onTrack = True
                else:
                    i += 1
                    if i < len(locs):
                        currLoc = locs[i]
                    else:
                        # Deviated completely from leg, but could rejoin on future leg
                        legFinished = True
                        i = lastTrackLocation + 1
                        currLoc = locs[i]
                        if onTrack:
                            numDeviations += 1
                            onTrack = False
                            deviatedLinks.append(leg['route'])
                            
    if not onTrack:
        timestampsDeviated += len(locs) - lastTrackLocation - 1
    if dis(locs[-1]['lat'], locs[-1]['lon'], monitoredTrip['to']['lat'], monitoredTrip['to']['lon']) < FINDIST:
        completed = True
    else:
        completed = False

    return {'deviatedLinks': deviatedLinks, 'timestampsDeviated': timestampsDeviated, 'numDeviations': numDeviations, 'completed': completed}
    
    
