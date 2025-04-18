def set_unscheduled_events_impedance(event_type, event_severity, sidewalk_crosswalk):

    factor = 0
    effect_type = ""

    # set the unscheduled events impedance factors and effect types
    if event_type == "Signals":

        if sidewalk_crosswalk == "CROSSWALK":

            # set impedance factor for 3 minutes or 0.05 hours and type ADD for crosswalk node
            factor = 0.05
            effect_type = "ADD"

    else:

        # set impedance factor for 2 x severity and type MUL for both sidewalk and crosswalk nodes
        factor = 2 * event_severity
        effect_type = "MUL"

    return factor, effect_type


def set_scheduled_events_impedance(event_subtype):

    factor = 0
    effect_type = ""

    # set the scheduled events impedance factors and effect types based on event subtypes
    subtype_impedance = {
        "Construction (Roadwork)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Emergency Roadwork (Roadwork)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Maintenance Activity (Roadwork)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Rolling Closures (Roadwork)": {"FACTOR": 1.25, "EFFECT_TYPE": "MUL"},
        "ATL UTD (Major Event)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Braves (Major Event)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Falcons (Major Event)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Hawks (Major Event)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Sporting Event (Major Event)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "(Road Race)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Peachtree (Road Race)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "Other (Road Race)": {"FACTOR": 2, "EFFECT_TYPE": "MUL"}, # do exact match for this
        "Crash Investigation": {"FACTOR": 2, "EFFECT_TYPE": "MUL"},
        "(Police Operations)": {"FACTOR": 3, "EFFECT_TYPE": "MUL"},
        "Other (Police Operations)": {"FACTOR": 3, "EFFECT_TYPE": "MUL"} # do exact match for this
    }

    subtype_keys = subtype_impedance.keys()

    if "Other" in event_subtype:

        if event_subtype == "Other (Road Race)" or event_subtype == "Other (Police Operations)":

            # do exact match with the Other subtype
            factor = subtype_impedance[event_subtype]["FACTOR"]
            effect_type = subtype_impedance[event_subtype]["EFFECT_TYPE"]
    
    else:

        for key in subtype_keys:

            if event_subtype in key:

                # match subtype partially
                factor = subtype_impedance[key]["FACTOR"]
                effect_type = subtype_impedance[key]["EFFECT_TYPE"]

                break

    return factor, effect_type