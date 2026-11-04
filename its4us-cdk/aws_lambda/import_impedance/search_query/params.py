def find_params(event):

    PARAMS = dict()

    QUERY_URL = event["QUERY_URL"]

    IMPEDANCE = "IMPEDANCE"
    IMPEDANCE_LINK_ATTRS = {
        "__lat": "__lat",
        "__lon": "__lon",
    }

    PARAMS = {
        "QUERY_URL": QUERY_URL,
        "IMPEDANCE": IMPEDANCE,
        "IMPEDANCE_LINK_ATTRS": IMPEDANCE_LINK_ATTRS,
    }

    return PARAMS