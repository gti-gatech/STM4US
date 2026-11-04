import sys
from unittest.mock import MagicMock

sys.modules["neo4j"] = MagicMock()

from calculate_impedance import calculate_impedance


class TestApplyWazeFactors:

    def test_waze_multiplicative_factors(self, constants, base_impedance_data, 
                                        waze_mul_data):
        
        PARAMS = constants
        baseImpedance = base_impedance_data
        waze_df = waze_mul_data
        
        travelTypes = [
            "None", "Some", "Device", "WChairM", "WChairE", "MScooter",
            "LowVision", "Blind", "Some-LowVision", "Device-LowVision",
            "WChairM-LowVision", "WChairE-LowVision", "MScooter-LowVision",
            "Some-Blind", "Device-Blind", "WChairM-Blind", "WChairE-Blind",
            "MScooter-Blind"
        ]
        
        impedance, errors = calculate_impedance(PARAMS, travelTypes, baseImpedance, waze_df)
        
        assert isinstance(errors, list)
        assert len(errors) == 0, "No errors expected for valid data"
        assert not impedance.empty
        assert "200001" in impedance["stmAdaPathLinkID"].values
        assert "200002" in impedance["stmAdaPathLinkID"].values
        
        mul_factor = 1.5 * 1.2
        
        link_200001 = impedance[impedance["stmAdaPathLinkID"] == "200001"]
        link_200002 = impedance[impedance["stmAdaPathLinkID"] == "200002"]
        
        for travel_type in travelTypes:
            expected_200001 = baseImpedance[0].data()[travel_type] * mul_factor
            actual_200001 = link_200001[travel_type].iloc[0]
            
            assert abs(actual_200001 - expected_200001) < 0.01, \
                f"Link 200001, {travel_type}: expected {expected_200001}, got {actual_200001}"
            
            expected_200002 = baseImpedance[1].data()[travel_type]
            actual_200002 = link_200002[travel_type].iloc[0]
            
            assert abs(actual_200002 - expected_200002) < 0.01, \
                f"Link 200002, {travel_type}: expected {expected_200002}, got {actual_200002}"


    def test_waze_additive_factors(self, constants, base_impedance_data, 
                                   waze_add_data):
        
        PARAMS = constants
        baseImpedance = base_impedance_data
        waze_df = waze_add_data
        
        travelTypes = [
            "None", "Some", "Device", "WChairM", "WChairE", "MScooter",
            "LowVision", "Blind", "Some-LowVision", "Device-LowVision",
            "WChairM-LowVision", "WChairE-LowVision", "MScooter-LowVision",
            "Some-Blind", "Device-Blind", "WChairM-Blind", "WChairE-Blind",
            "MScooter-Blind"
        ]
        
        impedance, errors = calculate_impedance(PARAMS, travelTypes, baseImpedance, waze_df)
        
        assert not impedance.empty
        assert "200001" in impedance["stmAdaPathLinkID"].values
        assert "200002" in impedance["stmAdaPathLinkID"].values
        
        add_factor = 5.0 + 3.0
        
        link_200001 = impedance[impedance["stmAdaPathLinkID"] == "200001"]
        link_200002 = impedance[impedance["stmAdaPathLinkID"] == "200002"]
        
        for travel_type in travelTypes:
            expected_200001 = baseImpedance[0].data()[travel_type] + add_factor
            actual_200001 = link_200001[travel_type].iloc[0]
            
            assert abs(actual_200001 - expected_200001) < 0.01, \
                f"Link 200001, {travel_type}: expected {expected_200001}, got {actual_200001}"
            
            expected_200002 = baseImpedance[1].data()[travel_type]
            actual_200002 = link_200002[travel_type].iloc[0]
            
            assert abs(actual_200002 - expected_200002) < 0.01, \
                f"Link 200002, {travel_type}: expected {expected_200002}, got {actual_200002}"


    def test_waze_combined_mul_and_add_factors(self, constants, base_impedance_data,
                                               waze_combined_data):
        
        PARAMS = constants
        baseImpedance = base_impedance_data
        waze_df = waze_combined_data
        
        travelTypes = [
            "None", "Some", "Device", "WChairM", "WChairE", "MScooter",
            "LowVision", "Blind", "Some-LowVision", "Device-LowVision",
            "WChairM-LowVision", "WChairE-LowVision", "MScooter-LowVision",
            "Some-Blind", "Device-Blind", "WChairM-Blind", "WChairE-Blind",
            "MScooter-Blind"
        ]
        
        impedance, errors = calculate_impedance(PARAMS, travelTypes, baseImpedance, waze_df)
        
        assert not impedance.empty
        assert "200001" in impedance["stmAdaPathLinkID"].values
        assert "200002" in impedance["stmAdaPathLinkID"].values
        
        mul_factor = 1.5 * 1.2
        add_factor = 5.0
        
        link_200001 = impedance[impedance["stmAdaPathLinkID"] == "200001"]
        link_200002 = impedance[impedance["stmAdaPathLinkID"] == "200002"]
        
        for travel_type in travelTypes:
            expected_200001 = (baseImpedance[0].data()[travel_type] * mul_factor) + add_factor
            actual_200001 = link_200001[travel_type].iloc[0]
            
            assert abs(actual_200001 - expected_200001) < 0.01, \
                f"Link 200001, {travel_type}: expected {expected_200001}, got {actual_200001}"
            
            expected_200002 = baseImpedance[1].data()[travel_type]
            actual_200002 = link_200002[travel_type].iloc[0]
            
            assert abs(actual_200002 - expected_200002) < 0.01, \
                f"Link 200002, {travel_type}: expected {expected_200002}, got {actual_200002}"
