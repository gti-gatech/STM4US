import sys
from unittest.mock import MagicMock
import pandas as pd

sys.modules["neo4j"] = MagicMock()

from calculate_impedance import calculate_impedance


class TestBaseImpedanceCalculation:

    def test_base_travel_time_calculation(self, constants, factors_data, simple_sidewalk_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = simple_sidewalk_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert isinstance(errors, list)
        assert len(errors) == 0, "No errors expected for valid data"
        assert not impedance.empty
        assert "100001" in impedance["stmAdaPathLinkID"].values
        
        length = 0.01
        base_time_none = (length * 3600) / speeds["None"]
        actual_time_none = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100001", "None"
        ].iloc[0]
        
        assert actual_time_none > 0
        assert abs(actual_time_none - base_time_none) < 0.01, \
            f"Expected {base_time_none}, got {actual_time_none}"


    def test_multiplicative_factors_applied(self, constants, factors_data, 
                                           sidewalk_with_attributes_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_attributes_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        assert "100002" in impedance["stmAdaPathLinkID"].values
        
        length = 0.02
        base_time_none = (length * 3600) / speeds["None"]
        
        actual_time_none = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100002", "None"
        ].iloc[0]
        
        width_factor = 1.2
        slope_factor = 1.1
        material_factor = 1.1
        expected_time = base_time_none * width_factor * slope_factor * material_factor
        
        assert actual_time_none > base_time_none, \
            f"Impedance with factors ({actual_time_none}) should be > base time ({base_time_none})"
        assert abs(actual_time_none - expected_time) < 0.01, \
            f"Expected {expected_time}, got {actual_time_none}"


    def test_additive_factors_applied(self, constants, factors_data, 
                                     simple_sidewalk_data, ramp_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = simple_sidewalk_data
        defects = []
        ramps = ramp_data
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        
        length = 0.01
        base_time_device = (length * 3600) / speeds["Device"]
        
        actual_time_device = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100001", "Device"
        ].iloc[0]
        
        ramp_type_add_seconds = 3
        ramp_width_add_seconds = 12
        expected_time = base_time_device + ramp_type_add_seconds + ramp_width_add_seconds
        
        assert actual_time_device > base_time_device, \
            f"Impedance with ramp ({actual_time_device}) should be > base time ({base_time_device})"
        assert abs(actual_time_device - expected_time) < 0.01, \
            f"Expected {expected_time}, got {actual_time_device}"


    def test_combined_multiplicative_and_additive_factors(self, constants, factors_data,
                                                          sidewalk_with_attributes_and_ramp_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_attributes_and_ramp_data["sidewalks"]
        defects = []
        ramps = sidewalk_with_attributes_and_ramp_data["ramps"]
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        assert "100003" in impedance["stmAdaPathLinkID"].values
        
        length = 0.015
        base_time_device = (length * 3600) / speeds["Device"]
        
        width_factor = 1.2
        slope_factor = 1.1
        material_factor = 1.1
        multiplied_time = base_time_device * width_factor * slope_factor * material_factor
        
        ramp_type_add = 3
        ramp_width_add = 12
        expected_time = multiplied_time + ramp_type_add + ramp_width_add
        
        actual_time_device = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100003", "Device"
        ].iloc[0]
        
        assert actual_time_device > base_time_device, \
            f"Combined impedance ({actual_time_device}) should be > base time ({base_time_device})"
        assert abs(actual_time_device - expected_time) < 0.1, \
            f"Expected {expected_time}, got {actual_time_device}"


    def test_multiple_ramps_on_same_link(self, constants, factors_data,
                                        sidewalk_with_multiple_ramps_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_multiple_ramps_data["sidewalks"]
        defects = []
        ramps = sidewalk_with_multiple_ramps_data["ramps"]
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        assert "100004" in impedance["stmAdaPathLinkID"].values
        
        length = 0.01
        base_time_device = (length * 3600) / speeds["Device"]
        
        ramp1_type_add = 3
        ramp1_width_add = 12
        ramp2_type_add = 15
        ramp2_width_add = 12
        total_ramp_add = ramp1_type_add + ramp1_width_add + ramp2_type_add + ramp2_width_add
        expected_time = base_time_device + total_ramp_add
        
        actual_time_device = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100004", "Device"
        ].iloc[0]
        
        assert actual_time_device > base_time_device, \
            f"Multiple ramps ({actual_time_device}) should be > base time ({base_time_device})"
        assert actual_time_device > 40, \
            f"Multiple ramps should add significant time, got {actual_time_device}"


    def test_empty_sidewalk_data(self, constants, factors_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = []
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert impedance.empty, "Empty sidewalk data should return empty DataFrame"
        assert isinstance(errors, list)
        assert len(errors) == 1, "Should have one error for empty data"
        assert errors[0]["type"] == "EmptyDataWarning"
        assert "No sidewalk data" in errors[0]["message"]


    def test_constraint_boundaries_inbx(self, constants, factors_data,
                                       sidewalk_with_boundary_values_data):
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_boundary_values_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        assert "100005" in impedance["stmAdaPathLinkID"].values
        
        length = 0.01
        base_time_none = (length * 3600) / speeds["None"]
        
        actual_time_none = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100005", "None"
        ].iloc[0]
        
        # width=30 matches INBX [0.001, 30] -> factor 1.2
        # slope=5.0 matches INBX [5, 100] -> factor 1.2
        width_factor = 1.2
        slope_factor = 1.2
        expected_time = base_time_none * width_factor * slope_factor
        
        assert abs(actual_time_none - expected_time) < 0.01, \
            f"Expected {expected_time}, got {actual_time_none}"


    def test_multiple_sidewalks_different_factors(self, constants, factors_data,
                                                  multiple_sidewalks_different_factors_data):
        """Test multiple sidewalks with different factor combinations in one calculation"""
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = multiple_sidewalks_different_factors_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        assert len(impedance) == 3, "Should have 3 sidewalk links"
        
        # Sidewalk 1: width=25, slope=4.0, material=Asphalt
        link_1 = impedance[impedance["stmAdaPathLinkID"] == "100010"]
        assert not link_1.empty
        base_time_1 = (0.01 * 3600) / speeds["None"]
        expected_1 = base_time_1 * 1.2 * 1.1 * 1.1  # width * slope * material
        actual_1 = link_1["None"].iloc[0]
        assert abs(actual_1 - expected_1) < 0.01, \
            f"Link 100010: expected {expected_1}, got {actual_1}"
        
        # Sidewalk 2: width=0 (EQ constraint factor), material=Concrete
        link_2 = impedance[impedance["stmAdaPathLinkID"] == "100011"]
        assert not link_2.empty
        base_time_2 = (0.02 * 3600) / speeds["None"]
        expected_2 = base_time_2 * 2 * 1  # width EQ 0 factor=2, concrete factor=1
        actual_2 = link_2["None"].iloc[0]
        assert abs(actual_2 - expected_2) < 0.01, \
            f"Link 100011: expected {expected_2}, got {actual_2}"
        
        # Sidewalk 3: no factors (just base time)
        link_3 = impedance[impedance["stmAdaPathLinkID"] == "100012"]
        assert not link_3.empty
        base_time_3 = (0.015 * 3600) / speeds["None"]
        actual_3 = link_3["None"].iloc[0]
        assert abs(actual_3 - base_time_3) < 0.01, \
            f"Link 100012: expected {base_time_3}, got {actual_3}"


    def test_eq_constraint_for_numeric_values(self, constants, factors_data,
                                              sidewalk_with_eq_zero_width_data):
        """Test EQ constraint on numeric (non-enumerated) factors"""
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_eq_zero_width_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        
        base_time_none = (0.01 * 3600) / speeds["None"]
        actual_time_none = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100020", "None"
        ].iloc[0]
        
        # Width=0 should trigger EQ constraint with factor=2 for "None"
        width_eq_factor = 2
        expected_time = base_time_none * width_eq_factor
        
        assert abs(actual_time_none - expected_time) < 0.01, \
            f"Width=0 should apply EQ factor. Expected {expected_time}, got {actual_time_none}"


    def test_no_factor_matches_out_of_range(self, constants, factors_data,
                                           sidewalk_with_out_of_range_values_data):
        """Test values outside all factor ranges - should use default (no factor applied)"""
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_out_of_range_values_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        
        base_time_none = (0.01 * 3600) / speeds["None"]
        actual_time_none = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100030", "None"
        ].iloc[0]
        
        # Width=100 and slope=2.0 are both outside factor ranges
        # Should just get base time (no factors applied)
        assert abs(actual_time_none - base_time_none) < 0.01, \
            f"Out of range values should give base time. Expected {base_time_none}, got {actual_time_none}"


    def test_null_and_missing_values(self, constants, factors_data,
                                    sidewalk_with_null_values_data):
        """Test handling of null/missing attribute values"""
        
        PARAMS = constants
        factors = factors_data
        
        travelTypes = factors.columns.tolist()[PARAMS["NUM_TRAVEL_TYPES"]:]
        speeds = factors[travelTypes].iloc[0]
        
        sidewalks = sidewalk_with_null_values_data
        defects = []
        ramps = []
        curbs = []
        curbCuts = []
        crossings = []
        busStops = []
        
        impedance, errors = calculate_impedance(
            PARAMS, factors, travelTypes, speeds, sidewalks, defects,
            ramps, curbs, curbCuts, crossings, busStops
        )
        
        assert isinstance(impedance, pd.DataFrame)
        assert not impedance.empty
        
        base_time_none = (0.01 * 3600) / speeds["None"]
        actual_time_none = impedance.loc[
            impedance["stmAdaPathLinkID"] == "100040", "None"
        ].iloc[0]
        
        # Null/missing values should be treated as not matching any factors
        # Should just get base time
        assert abs(actual_time_none - base_time_none) < 0.01, \
            f"Null values should give base time. Expected {base_time_none}, got {actual_time_none}"
