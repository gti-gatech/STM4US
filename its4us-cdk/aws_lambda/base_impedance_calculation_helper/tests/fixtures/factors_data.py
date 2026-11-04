import pytest
import pandas as pd


@pytest.fixture(scope="session")
def factors_data():
    data = {
        "Variable Name": [
            "ADA Mobility Mode Speed",
            "stmPedLinkLength",
            "stmPedLinkWidth",
            "stmPedLinkWidth",
            "stmPedLinkSlopeA_B",
            "stmPedLinkSlopeA_B",
            "stmPedLinkMaterial",
            "stmPedLinkMaterial",
            "stmPedRampType",
            "stmPedRampWidth",
        ],
        "Variable Description": [
            "Speed of ADA Mobility Mode (mph)",
            "Link length",
            "Link width",
            "Link width",
            "Link slope in direction A-B",
            "Link slope in direction A-B",
            "Link surface material",
            "Link surface material",
            "Ramp type",
            "Ramp width",
        ],
        "Format": [
            "NUMERIC", "NUMERIC", "NUMERIC", "NUMERIC", "NUMERIC", "NUMERIC",
            "TEXT", "TEXT", "TEXT", "NUMERIC"
        ],
        "Units": [
            "Miles/Hour", "Miles", "Inches", "Inches", "Degrees (decimal)",
            "Degrees (decimal)", "Enumerated", "Enumerated", "Enumerated", "Inches"
        ],
        "Enumeration": [
            "NA", "NA", "NA", "NA", "NA", "NA",
            "Concrete (Smooth)", " Asphalt", "Standard Perpendicular Ramp", "NA"
        ],
        "Impedance Effect Type": [
            "NA", "NA", "MUL", "MUL", "MUL", "MUL",
            "MUL", "MUL", "ADD", "ADD"
        ],
        "Impedance Equation Constraint": [
            "NA", "NA", "INBX", "EQ", "INBX", "INBX",
            "EQ", "EQ", "EQ", "INBX"
        ],
        "Lower Constraint Bound": [
            "NA", "NA", 0.001, 0, 3, 5,
            "Concrete (Smooth)", " Asphalt", "Standard Perpendicular Ramp", 1
        ],
        "Upper Constraint Bound": [
            "NA", "NA", 30, 0, 4.999, 100,
            "Concrete (Smooth)", " Asphalt", "Standard Perpendicular Ramp", 37
        ],
        "Calculation Notes": [
            "Use slowest value for mode speed",
            "NA",
            'Multiply base time by value if < 30"',
            "Multiply base time by value if = 0",
            "Multiply base time by value if > 3",
            "Multiply base time by value if > 5",
            "Multiply base time by value if present",
            "Multiply base time by value if present",
            "Add per Event",
            "Add if less than 36\"",
        ],
        "None": [2.5, "NA", 1.2, 2, 1.1, 1.2, 1, 1.1, 3, 0],
        "Some": [2, "NA", 1.2, 4, 1.1, 1.2, 1, 1.1, 3, 6],
        "Device": [1.5, "NA", 1.2, 4, 1.1, 1.2, 1, 1.1, 3, 12],
        "WChairM": [3, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
        "WChairE": [4, "NA", 2, 999, 1.1, 1.2, 1, 1.1, 3, 30],
        "MScooter": [5, "NA", 2, 999, 1.1, 1.2, 1, 1.1, 3, 30],
        "LowVision": [2, "NA", 1.2, 999, 1.1, 1.2, 1, 1.2, 3, 15],
        "Blind": [1.5, "NA", 1.2, 999, 1.1, 1.2, 1, 1.2, 3, 15],
        "Some-LowVision": [2, "NA", 1.2, 999, 1.1, 1.2, 1, 1.2, 3, 15],
        "Device-LowVision": [1.5, "NA", 1.2, 999, 1.1, 1.2, 1, 1.3, 3, 15],
        "WChairM-LowVision": [2, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
        "WChairE-LowVision": [2, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
        "MScooter-LowVision": [2, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
        "Some-Blind": [2, "NA", 1.2, 999, 1.1, 1.2, 1, 1.2, 3, 15],
        "Device-Blind": [1.5, "NA", 1.2, 999, 1.1, 1.2, 1, 1.3, 3, 15],
        "WChairM-Blind": [2, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
        "WChairE-Blind": [1.5, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
        "MScooter-Blind": [2, "NA", 2, 999, 1.1, 1.2, 1, 1.2, 3, 30],
    }
    
    return pd.DataFrame(data)
