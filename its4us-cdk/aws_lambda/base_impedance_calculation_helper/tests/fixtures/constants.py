import pytest


@pytest.fixture(scope="session")
def constants():

    NUM_TRAVEL_TYPES = -18
    FACTORS_FILE_KEY = "factors.csv"
    MUL = "MUL"
    ADD = "ADD"

    SIDEWALK_LINK_ATTRS = {
        "ID(na)": "ID(na)",
        "ID(nb)": "ID(nb)",
        "__datasetid": "__datasetid",
        "Timestamp": "Timestamp",
        "stmAdaPathLinkID": "stmAdaPathLinkID",
        "stmAdaPathLinkLength": "stmAdaPathLinkLength",
        "stmAssetDefectReportPedLinkID": "stmAssetDefectReportPedLinkID",
    }

    FACTORS_FIELDS = {
        "Impedance Effect Type": "Impedance Effect Type",
        "Units": "Units",
        "Enumeration": "Enumeration",
        "Enumerated": "Enumerated",
        "Variable Name": "Variable Name",
        "Lower Constraint Bound": "Lower Constraint Bound",
        "Upper Constraint Bound": "Upper Constraint Bound",
    }

    FLOAT_DATATYPE = "float"
    FLOAT64_DATATYPE = "float64"
    INT_DATATYPE = "int"
    INT32_DATATYPE = "int32"
    STR_DATATYPE = "str"

    CONSTANTS = {
        "NUM_TRAVEL_TYPES": NUM_TRAVEL_TYPES,
        "FACTORS_FILE_KEY": FACTORS_FILE_KEY,
        "MUL": MUL,
        "ADD": ADD,
        "SIDEWALK_LINK_ATTRS": SIDEWALK_LINK_ATTRS,
        "FACTORS_FIELDS": FACTORS_FIELDS,
        "FLOAT_DATATYPE": FLOAT_DATATYPE,
        "FLOAT64_DATATYPE": FLOAT64_DATATYPE,
        "INT_DATATYPE": INT_DATATYPE,
        "INT32_DATATYPE": INT32_DATATYPE,
        "STR_DATATYPE": STR_DATATYPE,
    }

    return CONSTANTS
