import pytest


@pytest.fixture(scope="session")
def constants():

    # define constants used
    # function-specific constants
    
    SCHEDULED_PREFIX = "scheduled_event_"
    UNSCHEDULED_PREFIX = "unscheduled_event_"
    COMMENT_PREFIX = "event_comment_"
    PROPERTY_PREFIX = "event_property_"

    EVENT_NODE_LABEL = "NAVIGATOR-EVENT"
    COMMENT_NODE_LABEL = "NAVIGATOR-EVENT-COMMENT"
    PROPERTY_NODE_LABEL = "NAVIGATOR-EVENT-PROPERTY"
    OSM_NODE_LABEL = "OSM-NODE"
    OSM_WAY_LABEL = "OSM-WAY"
    SIDEWALK_LABEL = "SIDEWALK"
    CROSSWALK_LABEL = "CROSSWALK"

    EVENT_HOLDTIME = 900000 # holding event data for 15 minutes or 900000 ms for DELETE request
    ATTACH_RADIUS = 50.0 # distance (ft) boundary for sidewalk/crosswalk node attachments
    METER_TO_FEET = 3.28084 # 1 meter is 3.28084 feet

    STUDY_AREA = {
        "34.0N84.4W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.4, "max_lon": -84.3},
        "33.8N84.4W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.4, "max_lon": -84.3},
        "33.9N84.4W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.4, "max_lon": -84.3},
        "33.8N84.1W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.1, "max_lon": -84.0},
        "34.0N84.3W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.3, "max_lon": -84.2},
        "33.8N84.3W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.3, "max_lon": -84.2},
        "33.9N84.3W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.3, "max_lon": -84.2},
        "33.8N84.2W": {"min_lat": 33.8, "max_lat": 33.9, "min_lon": -84.2, "max_lon": -84.1},
        "33.9N84.2W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.2, "max_lon": -84.1},
        "34.0N84.2W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.2, "max_lon": -84.1},
        "33.9N84.1W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.1, "max_lon": -84.0},
        "34.0N84.1W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.1, "max_lon": -84.0},
        "33.9N84.0W": {"min_lat": 33.9, "max_lat": 34.0, "min_lon": -84.0, "max_lon": -83.9},
        "34.0N84.0W": {"min_lat": 34.0, "max_lat": 34.1, "min_lon": -84.0, "max_lon": -83.9},
    }

    STUDY_AREA_BOUNDARY = {
        "min_lat": "min_lat",
        "max_lat": "max_lat",
        "min_lon": "min_lon",
        "max_lon": "max_lon",
    }

    EVENT_FIELDS = {
        "event_id": "event_id",
        "comment_id": "comment_id",
        "property_id": "property_id",
        "external_id": "external_id",
        "version": "version",
        "type": "type",
        "subtype": "subtype",
        "severity": "severity",
        "latitude": "latitude",
        "longitude": "longitude",
        "road_type": "road_type",
        "created_by": "created_by",
        "modified_date": "modified_date",
        "__modified_date_ms": "__modified_date_ms",
    }

    EVENT_NODE_ATTRS = {
        "__datasetid": "__datasetid",
        "__impedance_factor": "__impedance_factor",
        "__impedance_effect_type": "__impedance_effect_type",
        "__modified_date_ms": "__modified_date_ms",
    }

    EVENT_FIELD_DATATYPES = {"version": "int", "severity": "int", "latitude": "float", 
                             "longitude": "float", "__modified_date_ms": "float"}
    
    FOOTWAY_FIELDS = {"sidewalk": "sidewalk", "crossing": "crossing"}
    
    VERSION_DATATYPE = {"version": "int"}

    UNSCHEDULED = "UNSCHEDULED"
    SCHEDULED = "SCHEDULED"
    WAZE = "Waze"
    INTERSTATE = "Interstate"
    DATETIME_FORMAT_NO_DASH = "%Y%m%dT%H"
    DATETIME_FORMAT_WITH_DASH = "%Y-%m-%d %H:%M:%S.%f"
    CSV_EXTENSION = "csv"
    TIMEZONE = "US/Eastern"
    GEOSPACE = "WGS84"

    CONSTANTS = {
        "SCHEDULED_PREFIX": SCHEDULED_PREFIX,
        "UNSCHEDULED_PREFIX": UNSCHEDULED_PREFIX,
        "COMMENT_PREFIX": COMMENT_PREFIX,
        "PROPERTY_PREFIX": PROPERTY_PREFIX,
        "EVENT_NODE_LABEL": EVENT_NODE_LABEL,
        "COMMENT_NODE_LABEL": COMMENT_NODE_LABEL,
        "PROPERTY_NODE_LABEL": PROPERTY_NODE_LABEL,
        "OSM_NODE_LABEL": OSM_NODE_LABEL,
        "OSM_WAY_LABEL": OSM_WAY_LABEL,
        "SIDEWALK_LABEL": SIDEWALK_LABEL,
        "CROSSWALK_LABEL": CROSSWALK_LABEL,
        "EVENT_HOLDTIME": EVENT_HOLDTIME,
        "ATTACH_RADIUS": ATTACH_RADIUS,
        "METER_TO_FEET": METER_TO_FEET,
        "STUDY_AREA": STUDY_AREA,
        "STUDY_AREA_BOUNDARY": STUDY_AREA_BOUNDARY,
        "EVENT_FIELDS": EVENT_FIELDS,
        "EVENT_NODE_ATTRS": EVENT_NODE_ATTRS,
        "EVENT_FIELD_DATATYPES": EVENT_FIELD_DATATYPES,
        "FOOTWAY_FIELDS": FOOTWAY_FIELDS,
        "VERSION_DATATYPE": VERSION_DATATYPE,
        "UNSCHEDULED": UNSCHEDULED,
        "SCHEDULED": SCHEDULED,
        "WAZE": WAZE,
        "INTERSTATE": INTERSTATE,
        "DATETIME_FORMAT_NO_DASH": DATETIME_FORMAT_NO_DASH,
        "DATETIME_FORMAT_WITH_DASH": DATETIME_FORMAT_WITH_DASH,
        "CSV_EXTENSION": CSV_EXTENSION,
        "TIMEZONE": TIMEZONE,
        "GEOSPACE": GEOSPACE,
    }

    return CONSTANTS

