def set_waze_impedance(sidewalk_label, crosswalk_label):

    # set up the waze impedance factors and effect types in a dictionary

    sidewalk_effect_type = sidewalk_label + "_EFFECT_TYPE"
    crosswalk_effect_type = crosswalk_label + "_EFFECT_TYPE"
    subtype_impedance = {
        "ACCIDENT_MINOR": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                           sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "ACCIDENT_MAJOR": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                           sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "ACCIDENT": {
            "NO_SUBTYPE": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                           sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
            },
        "HAZARD_ON_ROAD": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                           sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "HAZARD_ON_SHOULDER": {sidewalk_label: 2, 
                               sidewalk_effect_type: "MUL"},
        "HAZARD_ON_SHOULDER_CAR_STOPPED": {sidewalk_label: 60/3600.0,
                                           sidewalk_effect_type: "ADD"},
        "HAZARD_ON_SHOULDER_ANIMALS": {sidewalk_label: 60/3600.0,
                                       sidewalk_effect_type: "ADD"},
        "HAZARD_WEATHER_FOG": {sidewalk_label: 2, crosswalk_label: 2,
                               sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_HAIL": {sidewalk_label: 5, crosswalk_label: 5,
                                sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_HEAVY_RAIN": {sidewalk_label: 5, crosswalk_label: 5,
                                      sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_HEAVY_SNOW": {sidewalk_label: 5, crosswalk_label: 5,
                                      sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_FLOOD": {sidewalk_label: 10, crosswalk_label: 10,
                                 sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_MONSOON": {sidewalk_label: 10, crosswalk_label: 10,
                                   sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_TORNADO": {sidewalk_label: 20, crosswalk_label: 20,
                                   sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_HEAT_WAVE": {sidewalk_label: 2, crosswalk_label: 2,
                                     sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_HURRICANE": {sidewalk_label: 20, crosswalk_label: 20,
                                     sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_WEATHER_FREEZING_RAIN": {sidewalk_label: 5, crosswalk_label: 5,
                                         sidewalk_effect_type: "MUL", crosswalk_effect_type: "MUL"},
        "HAZARD_ON_ROAD_LANE_CLOSED": {sidewalk_label: 60/3600.0, crosswalk_label: 270/3600.0,
                                       sidewalk_effect_type: "ADD", crosswalk_effect_type: "ADD"},
        "HAZARD_ON_ROAD_OIL": {sidewalk_label: 60/3600.0, crosswalk_label: 270/3600.0,
                               sidewalk_effect_type: "ADD", crosswalk_effect_type: "ADD"},
        "HAZARD_ON_ROAD_ICE": {sidewalk_label: 60/3600.0, crosswalk_label: 270/3600.0,
                               sidewalk_effect_type: "ADD", crosswalk_effect_type: "ADD"},
        "HAZARD_ON_ROAD_CONSTRUCTION": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                                        sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "HAZARD_ON_ROAD_CAR_STOPPED": {sidewalk_label: 60/3600.0, crosswalk_label: 270/3600.0,
                                       sidewalk_effect_type: "ADD", crosswalk_effect_type: "ADD"},
        "HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT": {crosswalk_label: 360/3600.0,
                                               crosswalk_effect_type: "ADD"},
        "CONSTRUCTION": {
            "NO_SUBTYPE": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                           sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
            },
        "ROAD_CLOSED_HAZARD": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                               sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "ROAD_CLOSED_CONSTRUCTION": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                                     sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "ROAD_CLOSED_EVENT": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                              sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        "ROAD_CLOSED": {
            "NO_SUBTYPE": {sidewalk_label: 2, crosswalk_label: 270/3600.0,
                           sidewalk_effect_type: "MUL", crosswalk_effect_type: "ADD"},
        }
    }

    return subtype_impedance