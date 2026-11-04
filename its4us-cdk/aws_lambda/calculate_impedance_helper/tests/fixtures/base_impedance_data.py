import pytest
from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def base_impedance_data():
    mock_record_1 = MagicMock()
    mock_record_1.data.return_value = {
        "stmAdaPathLinkID": 200001,
        "None": 10.0,
        "Some": 12.0,
        "Device": 15.0,
        "WChairM": 8.0,
        "WChairE": 6.0,
        "MScooter": 5.0,
        "LowVision": 14.0,
        "Blind": 18.0,
        "Some-LowVision": 13.0,
        "Device-LowVision": 16.0,
        "WChairM-LowVision": 9.0,
        "WChairE-LowVision": 7.0,
        "MScooter-LowVision": 6.0,
        "Some-Blind": 14.0,
        "Device-Blind": 17.0,
        "WChairM-Blind": 10.0,
        "WChairE-Blind": 8.0,
        "MScooter-Blind": 7.0,
    }
    
    mock_record_2 = MagicMock()
    mock_record_2.data.return_value = {
        "stmAdaPathLinkID": 200002,
        "None": 20.0,
        "Some": 22.0,
        "Device": 25.0,
        "WChairM": 18.0,
        "WChairE": 16.0,
        "MScooter": 15.0,
        "LowVision": 24.0,
        "Blind": 28.0,
        "Some-LowVision": 23.0,
        "Device-LowVision": 26.0,
        "WChairM-LowVision": 19.0,
        "WChairE-LowVision": 17.0,
        "MScooter-LowVision": 16.0,
        "Some-Blind": 24.0,
        "Device-Blind": 27.0,
        "WChairM-Blind": 20.0,
        "WChairE-Blind": 18.0,
        "MScooter-Blind": 17.0,
    }
    
    return [mock_record_1, mock_record_2]
