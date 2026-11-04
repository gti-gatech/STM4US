import pytest


@pytest.fixture(scope="session")
def constants():

    # define constants used
    # function-specific constants

    WAZE_DATA_FIELDS = {
        "alerts": "alerts",
        "startTimeMillis": "startTimeMillis",
        "endTimeMillis": "endTimeMillis",
        "startTime": "startTime",
        "endTime": "endTime",
    }

    WAZE_ALERT_FIELDS = {
        "location": "location",
        "x": "x",
        "y": "y",
        "uuid": "uuid",
        "type": "type",
        "subtype": "subtype",
        "roadType": "roadType",
        "endTime": "endTime",
        "endTimeMillis": "endTimeMillis",
    }

    WAZE_NODE_ATTRS = {
        "location-x": "location-x",
        "location-y": "location-y",
        "__datasetid": "__datasetid",
    }

    WAZE_LINK_ATTRS = {
        "__datasetid": "__datasetid",
        "__impedance_factor": "__impedance_factor",
        "__impedance_effect_type": "__impedance_effect_type",
    }
    
    NO_SUBTYPE = "NO_SUBTYPE"
    EFFECT_TYPE = "EFFECT_TYPE"
    HAZARD_WEATHER = "HAZARD_WEATHER"
    JAM = "JAM"
    WAZE_NODE_LABEL = "WAZE-ALERT"
    OSM_NODE_LABEL = "OSM-NODE"
    OSM_WAY_LABEL = "OSM-WAY"
    SIDEWALK_LABEL = "SIDEWALK"
    CROSSWALK_LABEL = "CROSSWALK"

    WAZE_HOLDTIME = 900000 # holding waze data for 15 minutes or 900000 ms for DELETE request
    WEATHER_DISTANCE = 1000.0 # distance (ft) boundary for sidewalk/crosswalk node attachments on weather hazard
    INTERSECTION_BOX = 300.0 # distance (ft) boundary for creating an intersection
    CROSSWALK_BOX_OUTSIDE = 80.0 # distance (ft) boundary for crosswalk node attachments on outside of intersection box
    CROSSWALK_BOX_OUTSIDE_SIDEWALK = 50.0 # distance (ft) boundary for sidealk node attachments on outside of intersection box
    CROSSWALK_BOX_INSIDE = 20.0 # distance (ft) boundary for crosswalk node attachments on inside of intersection box
    METER_TO_FEET = 3.28084 # 1 meter is 3.28084 feet

    GEOSPACE = "WGS84"


    CONSTANTS = {
        "WAZE_DATA_FIELDS": WAZE_DATA_FIELDS,
        "WAZE_ALERT_FIELDS": WAZE_ALERT_FIELDS,
        "WAZE_NODE_ATTRS": WAZE_NODE_ATTRS,
        "WAZE_LINK_ATTRS": WAZE_LINK_ATTRS,
        "WAZE_NODE_LABEL": WAZE_NODE_LABEL,
        "NO_SUBTYPE": NO_SUBTYPE,
        "EFFECT_TYPE": EFFECT_TYPE,
        "HAZARD_WEATHER": HAZARD_WEATHER,
        "JAM": JAM,
        "OSM_NODE_LABEL": OSM_NODE_LABEL,
        "OSM_WAY_LABEL": OSM_WAY_LABEL,
        "SIDEWALK_LABEL": SIDEWALK_LABEL,
        "CROSSWALK_LABEL": CROSSWALK_LABEL,
        "WAZE_HOLDTIME": WAZE_HOLDTIME,
        "WEATHER_DISTANCE": WEATHER_DISTANCE,
        "INTERSECTION_BOX": INTERSECTION_BOX,
        "CROSSWALK_BOX_OUTSIDE": CROSSWALK_BOX_OUTSIDE,
        "CROSSWALK_BOX_OUTSIDE_SIDEWALK": CROSSWALK_BOX_OUTSIDE_SIDEWALK,
        "CROSSWALK_BOX_INSIDE": CROSSWALK_BOX_INSIDE,
        "METER_TO_FEET": METER_TO_FEET,
        "GEOSPACE": GEOSPACE,
    }

    return CONSTANTS

