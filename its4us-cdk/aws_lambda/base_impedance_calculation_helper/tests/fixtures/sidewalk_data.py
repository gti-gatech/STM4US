import pytest
from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def simple_sidewalk_data():
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "stmAdaPathLinkID": 100001,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1000",
        "ID(nb)": "n1001",
        "__datasetid": "test-dataset",
    }
    return [mock_record]


@pytest.fixture(scope="session")
def sidewalk_with_attributes_data():
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "stmAdaPathLinkID": 100002,
        "stmAdaPathLinkLength": 0.02,
        "ID(na)": "n1002",
        "ID(nb)": "n1003",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 25,
        "stmPedLinkSlopeA_B": 4.0,
        "stmPedLinkMaterial": " Asphalt",
    }
    return [mock_record]


@pytest.fixture(scope="session")
def sidewalk_with_attributes_and_ramp_data():
    sidewalk_record = MagicMock()
    sidewalk_record.data.return_value = {
        "stmAdaPathLinkID": 100003,
        "stmAdaPathLinkLength": 0.015,
        "ID(na)": "n1004",
        "ID(nb)": "n1005",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 25,
        "stmPedLinkSlopeA_B": 4.0,
        "stmPedLinkMaterial": " Asphalt",
    }
    
    ramp_record = MagicMock()
    ramp_record.data.return_value = {
        "n": {
            "stmAssetDefectReportPedLinkID": 100003,
            "stmPedRampType": "Standard Perpendicular Ramp",
            "stmPedRampWidth": 30,
        }
    }
    
    return {"sidewalks": [sidewalk_record], "ramps": [ramp_record]}


@pytest.fixture(scope="session")
def sidewalk_with_multiple_ramps_data():
    sidewalk_record = MagicMock()
    sidewalk_record.data.return_value = {
        "stmAdaPathLinkID": 100004,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1006",
        "ID(nb)": "n1007",
        "__datasetid": "test-dataset",
    }
    
    ramp_record_1 = MagicMock()
    ramp_record_1.data.return_value = {
        "n": {
            "stmAssetDefectReportPedLinkID": 100004,
            "stmPedRampType": "Standard Perpendicular Ramp",
            "stmPedRampWidth": 30,
        }
    }
    
    ramp_record_2 = MagicMock()
    ramp_record_2.data.return_value = {
        "n": {
            "stmAssetDefectReportPedLinkID": 100004,
            "stmPedRampType": " Diagonal Ramp",
            "stmPedRampWidth": 25,
        }
    }
    
    return {"sidewalks": [sidewalk_record], "ramps": [ramp_record_1, ramp_record_2]}


@pytest.fixture(scope="session")
def sidewalk_with_boundary_values_data():
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "stmAdaPathLinkID": 100005,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1008",
        "ID(nb)": "n1009",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 30,
        "stmPedLinkSlopeA_B": 5.0,
    }
    return [mock_record]


@pytest.fixture(scope="session")
def ramp_data():
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "n": {
            "stmAssetDefectReportPedLinkID": 100001,
            "stmPedRampType": "Standard Perpendicular Ramp",
            "stmPedRampWidth": 30,
        }
    }
    return [mock_record]


@pytest.fixture(scope="session")
def empty_data():
    return []


@pytest.fixture(scope="session")
def multiple_sidewalks_different_factors_data():
    """Three sidewalks with different factor combinations"""
    
    # Sidewalk 1: width=25, slope=4.0, material=Asphalt (all in range)
    sidewalk_1 = MagicMock()
    sidewalk_1.data.return_value = {
        "stmAdaPathLinkID": 100010,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1010",
        "ID(nb)": "n1011",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 25,
        "stmPedLinkSlopeA_B": 4.0,
        "stmPedLinkMaterial": " Asphalt",
    }
    
    # Sidewalk 2: width=0 (EQ constraint), no slope, material=Concrete
    sidewalk_2 = MagicMock()
    sidewalk_2.data.return_value = {
        "stmAdaPathLinkID": 100011,
        "stmAdaPathLinkLength": 0.02,
        "ID(na)": "n1012",
        "ID(nb)": "n1013",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 0,
        "stmPedLinkMaterial": "Concrete (Smooth)",
    }
    
    # Sidewalk 3: no factors apply (simple base time)
    sidewalk_3 = MagicMock()
    sidewalk_3.data.return_value = {
        "stmAdaPathLinkID": 100012,
        "stmAdaPathLinkLength": 0.015,
        "ID(na)": "n1014",
        "ID(nb)": "n1015",
        "__datasetid": "test-dataset",
    }
    
    return [sidewalk_1, sidewalk_2, sidewalk_3]


@pytest.fixture(scope="session")
def sidewalk_with_eq_zero_width_data():
    """Sidewalk with width=0 to trigger EQ constraint factor"""
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "stmAdaPathLinkID": 100020,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1020",
        "ID(nb)": "n1021",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 0,
    }
    return [mock_record]


@pytest.fixture(scope="session")
def sidewalk_with_out_of_range_values_data():
    """Sidewalk with values outside all factor ranges"""
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "stmAdaPathLinkID": 100030,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1030",
        "ID(nb)": "n1031",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": 100,  # Outside INBX range [0.001, 30]
        "stmPedLinkSlopeA_B": 2.0,  # Below INBX range [3, 4.999]
    }
    return [mock_record]


@pytest.fixture(scope="session")
def sidewalk_with_null_values_data():
    """Sidewalk with missing/null attribute values"""
    mock_record = MagicMock()
    mock_record.data.return_value = {
        "stmAdaPathLinkID": 100040,
        "stmAdaPathLinkLength": 0.01,
        "ID(na)": "n1040",
        "ID(nb)": "n1041",
        "__datasetid": "test-dataset",
        "stmPedLinkWidth": None,
        "stmPedLinkSlopeA_B": "na",
    }
    return [mock_record]
